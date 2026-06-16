package sage.backend

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

import _root_.kyo.{<, Abort, Async, Duration, Frame, Maybe, Scope, Stream, Tag}
import _root_.kyo.Duration.toMillis
import _root_.kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, ScanStep, ScanTarget, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * The Kyo-native surface: the same client, with every method returning a Kyo pending computation.
  */
type SageClient = Client[[A] =>> A < (Abort[Throwable] & Async), String]

private type KyoEff[A] = A < (Abort[Throwable] & Async)

extension [K](client: Client[[A] =>> A < (Abort[Throwable] & Async), K])(using @unused ev: KeyCodec[K]) {

  /**
    * Runs a read with client-side caching and a Kyo `Duration` TTL — the Kyo-native form of [[sage.client.internal.Client.cached]].
    */
  def cached[A](command: Command[A], ttl: Duration): A < (Abort[Throwable] & Async) =
    client.cached(command, FiniteDuration(ttl.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once. In cluster
    * mode it walks every slot-owning master in turn, each with its own node-local cursor, so the sweep covers the whole keyspace.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using Tag[K]): Stream[K, Abort[Throwable] & Async] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[(F, V), Abort[Throwable] & Async] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[V, Abort[Throwable] & Async] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[(V, Double), Abort[Throwable] & Async] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // chunkSize = 1: emit each page as it arrives — the default rechunks to 4096, withholding an infinite tail (xTail/xConsume) until then
  private def paged[S, A](start: S)(step: S => Maybe[(Vector[A], S)] < (Abort[Throwable] & Async))(
    using Tag[A]
  ): Stream[A, Abort[Throwable] & Async] =
    Stream.unfold[S, Vector[A], Abort[Throwable] & Async](start, chunkSize = 1)(step).flatMap(items => Stream.init(items))

  private def scanStream[A](fetch: ScanCursor => KyoEff[ScanPage[A]])(using Tag[A]): Stream[A, Abort[Throwable] & Async] =
    paged[Option[ScanCursor], A](Some(ScanCursor.start)) {
      case None         => Maybe.empty
      case Some(cursor) => fetch(cursor).map(page => Maybe((page.items, page.next)))
    }

  // walks every scan target in turn, each with its own node-local cursor, so a cluster SCAN sweeps all masters instead of one
  private def scanStreamAll[A](fetch: ScanTarget => ScanCursor => KyoEff[ScanPage[A]])(using Tag[A]): Stream[A, Abort[Throwable] & Async] =
    paged[ScanStep, A](ScanStep.Begin) {
      case ScanStep.Begin                 =>
        client.scanTargets.map(targets => if (targets.isEmpty) Maybe.empty else Maybe((Vector.empty[A], ScanStep.Visit(ScanCursor.start, targets))))
      case ScanStep.Visit(cursor, remain) =>
        fetch(remain.head)(cursor).map { page =>
          page.next match {
            case Some(next) => Maybe((page.items, ScanStep.Visit(next, remain)))
            case None       => Maybe((page.items, if (remain.tail.isEmpty) ScanStep.End else ScanStep.Visit(ScanCursor.start, remain.tail)))
          }
        }
      case ScanStep.End                   => Maybe.empty
    }

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[Throwable] & Async] =
    paged[Option[StreamRangeId], StreamEntry[F, V]](Some(start)) {
      case None       => Maybe.empty
      case Some(from) =>
        client.run(Streams.xRange[K, F, V](key, from, end, Some(batch))).map { entries =>
          if (entries.isEmpty) Maybe.empty
          else Maybe((entries, if (entries.length < batch) None else Some(StreamRangeId.Exclusive(entries.last.id))))
        }
    }

  /**
    * Auto-claims idle pending entries for `consumer`, advancing the `XAUTOCLAIM` cursor each call until it wraps back to the start. Tombstone
    * entries — claimed ids whose data was already deleted, which the decoder surfaces with no fields — are skipped, so every emitted entry
    * carries data.
    */
  def xAutoClaimAll[F: KeyCodec, V: ValueCodec](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[Throwable] & Async] =
    paged[Option[StreamId], StreamEntry[F, V]](Some(start)) {
      case None       => Maybe.empty
      case Some(from) =>
        client.run(Streams.xAutoClaim[K, F, V](key, group, consumer, minIdle, from, count)).map { result =>
          Maybe((result.entries.filter(_.fields.nonEmpty), if (result.cursor == StreamId.Zero) None else Some(result.cursor)))
        }
    }

  /**
    * Follows a stream without a consumer group: replays every entry after `from`, then blocks for new entries forever, advancing past the
    * last id each round. Blocking on an explicit id (never `$`) leaves no gap for entries arriving mid-loop. No acknowledgement, unlike
    * [[xConsume]]. `from` defaults to the start; pass a later id to tail from there.
    */
  def xTail[F: KeyCodec, V: ValueCodec](
    key: K,
    from: StreamId = StreamId.Zero,
    count: Option[Long] = None,
    block: BlockTimeout = SageClient.defaultPoll
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[Throwable] & Async] =
    paged[StreamId, StreamEntry[F, V]](from) { last =>
      client.run(Streams.xRead[K, F, V]((key, ReadId.After(last)))(count = count, block = Some(block))).map { result =>
        val entries = result.flatMap(_._2)
        Maybe((entries, if (entries.isEmpty) last else entries.last.id))
      }
    }

  /**
    * Tails a consumer group: first drains this consumer's own pending history (at-least-once recovery after a restart), then blocks for new
    * entries forever. `handle` runs per entry; the entry is acknowledged only after `handle` succeeds, so a failure leaves it in the PEL for
    * recovery.
    */
  def xConsume[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long] = None,
    block: BlockTimeout = SageClient.defaultPoll
  )(handle: StreamEntry[F, V] => KyoEff[Unit])(using Tag[F], Tag[V], Frame): KyoEff[Unit] =
    consumeStream[F, V](group, consumer, key, count, block)
      .foreach(entry => handle(entry).flatMap(_ => client.run(Streams.xAck(key, group)(entry.id)).map(_ => ())))

  private def consumeStream[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[Throwable] & Async] =
    paged[Either[StreamId, Unit], StreamEntry[F, V]](Left(StreamId.Zero)) {
      case Left(after) =>
        client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.After(after)))(count = count)).map { result =>
          val entries = result.flatMap(_._2)
          if (entries.isEmpty) Maybe((Vector.empty, Right(()))) else Maybe((entries, Left(entries.last.id)))
        }
      case Right(_)    =>
        client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.New))(count = count, block = Some(block))).map { result =>
          Maybe((result.flatMap(_._2), Right(())))
        }
    }

  /**
    * Subscribes to one or more channels; closing the enclosing `Scope` unsubscribes. Survives reconnects via auto-resubscribe, dropping
    * messages published during the reconnect gap.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*)(using Tag[Message[V]], Frame): Stream[Message[V], Abort[Throwable] & Async & Scope] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*)(
    using Tag[PatternMessage[V]],
    Frame
  ): Stream[PatternMessage[V], Abort[Throwable] & Async & Scope] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more Shard Channels; in a cluster each is routed to the Node owning its Slot, and resubscription follows the Slot
    * on migration or failover. A sharded delivery is an ordinary [[Message]].
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*)(using Tag[Message[V]], Frame): Stream[Message[V], Abort[Throwable] & Async & Scope] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

  /**
    * Like [[subscribe]], but the returned effect settles only once the server has confirmed the SUBSCRIBE, so a publish sequenced after it
    * cannot race the registration. Closing the enclosing `Scope` unsubscribes.
    */
  def subscribeScoped[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[Throwable] & Async & Scope] < (Abort[Throwable] & Async & Scope) =
    scopedStreamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[pSubscribe]], but the returned effect settles only once the server has confirmed the PSUBSCRIBE. Closing the enclosing `Scope`
    * unsubscribes.
    */
  def pSubscribeScoped[V: ValueCodec](pattern: String, rest: String*)(
    using Tag[PatternMessage[V]],
    Frame
  ): Stream[PatternMessage[V], Abort[Throwable] & Async & Scope] < (Abort[Throwable] & Async & Scope) =
    scopedStreamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[sSubscribe]], but the returned effect settles only once the server has confirmed the SSUBSCRIBE. Closing the enclosing `Scope`
    * unsubscribes.
    */
  def sSubscribeScoped[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[Throwable] & Async & Scope] < (Abort[Throwable] & Async & Scope) =
    scopedStreamOf(client.subscribeShardChannels[V](channel, rest*))

  private def streamOf[A](
    open: => Subscription[KyoEff, A] < (Abort[Throwable] & Async)
  )(using Tag[A], Frame): Stream[A, Abort[Throwable] & Async & Scope] =
    Stream.init(Scope.acquireRelease(open)(_.close).map(Seq(_))).flatMap(deliveries)

  private def scopedStreamOf[A](
    open: => Subscription[KyoEff, A] < (Abort[Throwable] & Async)
  )(using Tag[A], Frame): Stream[A, Abort[Throwable] & Async & Scope] < (Abort[Throwable] & Async & Scope) =
    Scope.acquireRelease(open)(_.close).map(deliveries)

  // chunkSize = 1: emit each message as it arrives — the default rechunks to 4096, withholding a live stream until that many accumulate
  private def deliveries[A](sub: Subscription[KyoEff, A])(using Tag[A], Frame): Stream[A, Abort[Throwable] & Async & Scope] =
    Stream.repeatPresent(sub.next.map(opt => Maybe.fromOption(opt.map(Seq(_)))), chunkSize = 1)
}

object SageClient {

  /**
    * A command surface re-typed to a non-String key, as returned by `client.as[K]`. The unqualified [[SageClient]] is String-keyed;
    * use `as` to reach any other key type over the same connection.
    */
  type Keyed[K] = Client[[A] =>> A < (Abort[Throwable] & Async), K]

  // bounded poll so xConsume's blocking read returns periodically, keeping cancellation responsive
  private[backend] val defaultPoll: BlockTimeout = BlockTimeout.After(FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS))

  def connect(config: SageConfig): SageClient < (Abort[Throwable] & Async) =
    Client.connect(config).lower.map(new Lowered(_))

  def scoped(config: SageConfig): SageClient < (Scope & Abort[Throwable] & Async) =
    Scope.acquireRelease(connect(config))(client => Abort.run(client.close))

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[A] =>> A < (Abort[Throwable] & Async)](underlying) {
    protected def lower[A](c: CIO[A]): A < (Abort[Throwable] & Async) = c.lower
    protected def lift[A](fa: A < (Abort[Throwable] & Async)): CIO[A] = CIO.lift(fa)
  }
}
