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
  * A Sage client for Kyo. Each method returns a pending computation whose `Abort` channel contains [[SageException]], allowing callers to
  * handle every expected failure. Other throwables become Kyo `Panic` defects.
  */
type SageClient = Client[[A] =>> A < (Abort[SageException] & Async), String]

// keep SageException in the typed failure channel and convert other throwables to Panic.
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
    * Scans the full keyspace. An empty page does not end the scan; iteration stops when the server returns a zero cursor. Redis may return
    * the same key more than once. In cluster mode, each master is scanned with its own cursor.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using Tag[K]): Stream[K, Abort[SageException] & Async] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * Iterates over all HSCAN field/value pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[(F, V), Abort[SageException] & Async] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * Iterates over all SSCAN members until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[V, Abort[SageException] & Async] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * Iterates over all ZSCAN member/score pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[(V, Double), Abort[SageException] & Async] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // A chunk size of 1 emits each page immediately. Kyo's default chunk size of 4096 would delay an unbounded stream such as xTail or
  // xConsume. Paged provides the iteration logic; this adapter converts its CIO and Option result to Kyo types.
  private def paged[S, A](start: S)(step: Paged.Step[S, A])(using Tag[A]): Stream[A, Abort[SageException] & Async] =
    Stream
      .unfold[S, Vector[A], Abort[SageException] & Async](start, chunkSize = 1)(s => refine(step(s).lower).map(Maybe.fromOption))
      .flatMap(items => Stream.init(items))

  private def scanStream[A](fetch: ScanCursor => ScanPage[A] < (Abort[SageException] & Async))(
    using Tag[A]
  ): Stream[A, Abort[SageException] & Async] =
    paged[Option[ScanCursor], A](Some(ScanCursor.start))(Paged.byCursor(cursor => CIO.lift(fetch(cursor))))

  // scan each target with its own node-local cursor. A cluster has one target for every slot-owning master.
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
    * Auto-claims idle pending entries for `consumer`, advancing the `XAUTOCLAIM` cursor until it returns to the start. Entries whose data
    * has already been deleted are skipped.
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
    * Follows a stream without a consumer group. It first reads every entry after `from`, then waits for new entries. The explicit entry ID
    * used for each blocking read avoids missing entries that arrive between reads. Unlike [[xConsume]], this method does not acknowledge
    * entries. `from` defaults to the start of the stream.
    */
  def xTail[F: KeyCodec, V: ValueCodec](
    key: K,
    from: StreamId = StreamId.Zero,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
  )(using Tag[F], Tag[V]): Stream[StreamEntry[F, V], Abort[SageException] & Async] =
    paged[StreamId, StreamEntry[F, V]](from)(
      Paged.tail(last =>
        CIO.lift(client.run(Streams.xRead[K, F, V]((key, ReadId.After(last)))(count = count, block = Some(block)))).map(_.flatMap(_._2))
      )
    )

  /**
    * Follows a stream as part of a consumer group. It processes this consumer's pending entries first, then waits for new entries. Each
    * entry is acknowledged only after `handle` succeeds. If `handle` fails, the entry remains pending and can be recovered later.
    */
  def xConsume[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
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
    * Subscribes to one or more channels. Closing the enclosing `Scope` unsubscribes. Sage resubscribes after reconnecting, but messages
    * published while the connection is down are lost.
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
    * Like [[subscribe]], but opens the subscription before completing the effect. Standalone and master-replica clients wait for server
    * confirmation. Cluster clients wait up to the connection timeout and may return before confirmation, which can arrive later. Closing the enclosing
    * `Scope` unsubscribes.
    */
  def subscribeScoped[V: ValueCodec](channel: String, rest: String*)(
    using Tag[Message[V]],
    Frame
  ): Stream[Message[V], Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    scopedStreamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[pSubscribe]], but opens the subscription before completing the effect. Confirmation follows the same timeout behavior as
    * [[subscribeScoped]]. Closing the enclosing `Scope` unsubscribes.
    */
  def pSubscribeScoped[V: ValueCodec](pattern: String, rest: String*)(
    using Tag[PatternMessage[V]],
    Frame
  ): Stream[PatternMessage[V], Abort[SageException] & Async & Scope] < (Abort[SageException] & Async & Scope) =
    scopedStreamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[sSubscribe]], but opens the subscription before completing the effect. Confirmation follows the same timeout behavior as
    * [[subscribeScoped]]. Closing the enclosing `Scope` unsubscribes.
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

  // emit each message immediately. Kyo's default chunk size of 4096 would wait for that many messages before delivering them.
  private def deliveries[A](
    sub: Subscription[[B] =>> B < (Abort[SageException] & Async), A]
  )(using Tag[A], Frame): Stream[A, Abort[SageException] & Async & Scope] =
    Stream.repeatPresent(sub.next.map(opt => Maybe.fromOption(opt.map(Seq(_)))), chunkSize = 1)
}

object SageClient {

  /**
    * A client that uses `K` for keys, returned by `client.as[K]`. [[SageClient]] uses `String` keys by default. Calling `as` changes only
    * the key type and continues to use the same connection.
    */
  type Keyed[K] = Client[[A] =>> A < (Abort[SageException] & Async), K]
  def connect(config: SageConfig): SageClient < (Abort[SageException] & Async) =
    refine(Client.connect(config).lower).map(new Lowered(_))

  def scoped(config: SageConfig): SageClient < (Scope & Abort[SageException] & Async) =
    Scope.acquireRelease(connect(config))(client => Abort.run[SageException](client.close))

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[A] =>> A < (Abort[SageException] & Async)](underlying) {
    protected def lower[A](c: CIO[A]): A < (Abort[SageException] & Async) = refine(c.lower)
    protected def lift[A](fa: A < (Abort[SageException] & Async)): CIO[A] = CIO.lift(fa)
  }
}
