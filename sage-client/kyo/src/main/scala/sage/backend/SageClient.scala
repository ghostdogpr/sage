package sage.backend

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

import _root_.kyo.{<, Abort, Async, Duration, Frame, Maybe, Scope, Stream, Tag}
import _root_.kyo.Duration.toMillis
import _root_.kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, ScanStep, ScanTarget, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * The Kyo-native surface: the same client, with every method returning a pending computation whose `Abort` channel is the sealed
  * [[SageException]], so failures are matched exhaustively. Anything that is not a `SageException` is a Kyo `Panic` (a defect), never a
  * typed failure.
  */
type SageClient = Client[[A] =>> A < (Abort[SageException] & Async), String]

// Narrow the carrier to the typed channel: a SageException stays a failure, anything else becomes a Panic.
private val toSageException: Throwable => Nothing < Abort[SageException] = {
  case e: SageException => Abort.fail(e)
  case e                => Abort.panic(e)
}

private def refine[A](v: A < (Abort[Throwable] & Async))(using Frame): A < (Abort[SageException] & Async) =
  Abort.recover[Throwable](toSageException)(v)

extension [K](client: Client[[A] =>> A < (Abort[SageException] & Async), K])(using @unused ev: KeyCodec[K]) {

  /**
    * Runs a read with client-side caching and a Kyo `Duration` TTL — the Kyo-native form of [[sage.client.internal.Client.cached]].
    */
  def cached[A](command: Command[A], ttl: Duration): A < (Abort[SageException] & Async) =
    client.cached(command, FiniteDuration(ttl.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once. In cluster
    * mode it walks every slot-owning master in turn, each with its own node-local cursor, so the sweep covers the whole keyspace.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using Tag[K]): Stream[K, Abort[SageException] & Async] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[(F, V), Abort[SageException] & Async] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[V, Abort[SageException] & Async] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[(V, Double), Abort[SageException] & Async] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // chunkSize = 1: emit each page as it arrives — the default rechunks to 4096, withholding an infinite tail (xTail/xConsume) until then.
  // The page-step logic itself is shared in `Paged`; here we lower its `CIO` to Kyo, refine the channel, and turn the `Option` end-signal into a `Maybe`.
  private def paged[S, A](start: S)(step: Paged.Step[S, A])(using Tag[A]): Stream[A, Abort[SageException] & Async] =
    Stream
      .unfold[S, Vector[A], Abort[SageException] & Async](start, chunkSize = 1)(s => refine(step(s).lower).map(Maybe.fromOption))
      .flatMap(items => Stream.init(items))

  private def scanStream[A](fetch: ScanCursor => ScanPage[A] < (Abort[SageException] & Async))(
    using Tag[A]
  ): Stream[A, Abort[SageException] & Async] =
    paged[Option[ScanCursor], A](Some(ScanCursor.start))(Paged.byCursor(cursor => CIO.lift(fetch(cursor))))

  // walks every scan target in turn, each with its own node-local cursor, so a cluster SCAN sweeps all masters instead of one
  private def scanStreamAll[A](
    fetch: ScanTarget => ScanCursor => ScanPage[A] < (Abort[SageException] & Async)
  )(using Tag[A]): Stream[A, Abort[SageException] & Async] =
    paged[ScanStep, A](ScanStep.Begin)(Paged.acrossTargets(CIO.lift(client.scanTargets))(target => cursor => CIO.lift(fetch(target)(cursor))))

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[SageException] & Async] =
    paged[Option[StreamRangeId], StreamEntry[F, V]](Some(start))(
      Paged.byRange(batch)(from => CIO.lift(client.run(Streams.xRange[K, F, V](key, from, end, Some(batch)))))
    )

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
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[SageException] & Async] =
    paged[Option[StreamId], StreamEntry[F, V]](Some(start))(
      Paged.byAutoClaim(from => CIO.lift(client.run(Streams.xAutoClaim[K, F, V](key, group, consumer, minIdle, from, count))))
    )

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
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[SageException] & Async] =
    paged[StreamId, StreamEntry[F, V]](from)(
      Paged.tail(last =>
        CIO.lift(client.run(Streams.xRead[K, F, V]((key, ReadId.After(last)))(count = count, block = Some(block)))).map(_.flatMap(_._2))
      )
    )

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
  )(handle: StreamEntry[F, V] => Unit < (Abort[SageException] & Async))(using Tag[F], Tag[V], Frame): Unit < (Abort[SageException] & Async) =
    consumeStream[F, V](group, consumer, key, count, block)
      .foreach(entry => handle(entry).flatMap(_ => client.run(Streams.xAck(key, group)(entry.id)).map(_ => ())))

  private def consumeStream[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[SageException] & Async] =
    paged[Either[StreamId, Unit], StreamEntry[F, V]](Left(StreamId.Zero))(
      Paged.consume(
        drainPending = after =>
          CIO.lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.After(after)))(count = count))).map(_.flatMap(_._2)),
        tailNew = CIO
          .lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.New))(count = count, block = Some(block))))
          .map(_.flatMap(_._2))
      )
    )

  /**
    * Subscribes to one or more channels; closing the enclosing `Scope` unsubscribes. Survives reconnects via auto-resubscribe, dropping
    * messages published during the reconnect gap.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[SageException] & Async & Scope] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*)(
    using Tag[PatternMessage[V]],
    Frame
  ): Stream[PatternMessage[V], Abort[SageException] & Async & Scope] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more Shard Channels; in a cluster each is routed to the Node owning its Slot, and resubscription follows the Slot
    * on migration or failover. A sharded delivery is an ordinary [[Message]].
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[SageException] & Async & Scope] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

  /**
    * Like [[subscribe]], but the returned effect settles only once the server has confirmed the SUBSCRIBE, so a publish sequenced after it
    * cannot race the registration. Closing the enclosing `Scope` unsubscribes.
    */
  def subscribeScoped[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    scopedStreamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[pSubscribe]], but the returned effect settles only once the server has confirmed the PSUBSCRIBE. Closing the enclosing `Scope`
    * unsubscribes.
    */
  def pSubscribeScoped[V: ValueCodec](pattern: String, rest: String*)(
    using Tag[PatternMessage[V]],
    Frame
  ): Stream[PatternMessage[V], Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    scopedStreamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[sSubscribe]], but the returned effect settles only once the server has confirmed the SSUBSCRIBE. Closing the enclosing `Scope`
    * unsubscribes.
    */
  def sSubscribeScoped[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    scopedStreamOf(client.subscribeShardChannels[V](channel, rest*))

  private def streamOf[A](
    open: => Subscription[[B] =>> B < (Abort[SageException] & Async), A] < (Abort[SageException] & Async)
  )(using Tag[A], Frame): Stream[A, Abort[SageException] & Async & Scope] =
    Stream.init(Scope.acquireRelease(open)(_.close).map(Seq(_))).flatMap(deliveries)

  private def scopedStreamOf[A](
    open: => Subscription[[B] =>> B < (Abort[SageException] & Async), A] < (Abort[SageException] & Async)
  )(using Tag[A], Frame): Stream[A, Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    Scope.acquireRelease(open)(_.close).map(deliveries)

  // chunkSize = 1: emit each message as it arrives — the default rechunks to 4096, withholding a live stream until that many accumulate
  private def deliveries[A](
    sub: Subscription[[B] =>> B < (Abort[SageException] & Async), A]
  )(using Tag[A], Frame): Stream[A, Abort[SageException] & Async & Scope] =
    Stream.repeatPresent(sub.next.map(opt => Maybe.fromOption(opt.map(Seq(_)))), chunkSize = 1)
}

object SageClient {

  /**
    * A command surface re-typed to a non-String key, as returned by `client.as[K]`. The unqualified [[SageClient]] is String-keyed;
    * use `as` to reach any other key type over the same connection.
    */
  type Keyed[K] = Client[[A] =>> A < (Abort[SageException] & Async), K]

  // bounded poll so xConsume's blocking read returns periodically, keeping cancellation responsive
  private[backend] val defaultPoll: BlockTimeout = BlockTimeout.After(FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS))

  def connect(config: SageConfig): SageClient < (Abort[SageException] & Async) =
    refine(Client.connect(config).lower).map(new Lowered(_))

  def scoped(config: SageConfig): SageClient < (Scope & Abort[SageException] & Async) =
    Scope.acquireRelease(connect(config))(client => Abort.run[SageException](client.close))

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[A] =>> A < (Abort[SageException] & Async)](underlying) {
    protected def lower[A](c: CIO[A]): A < (Abort[SageException] & Async) = refine(c.lower)
    protected def lift[A](fa: A < (Abort[SageException] & Async)): CIO[A] = CIO.lift(fa)
  }
}
