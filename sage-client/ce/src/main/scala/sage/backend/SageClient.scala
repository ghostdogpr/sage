package sage.backend

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

import cats.effect.{IO, Resource}
import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, ScanStep, ScanTarget, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * A Sage client for Cats Effect. Its methods return `IO`.
  */
type SageClient = Client[IO, String]

extension [K](client: Client[IO, K])(using @unused ev: KeyCodec[K]) {

  /**
    * Scans the full keyspace. An empty page does not end the scan; iteration stops when the server returns a zero cursor. Redis may return
    * the same key more than once. In cluster mode, each master is scanned with its own cursor.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): fs2.Stream[IO, K] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * Iterates over all HSCAN field/value pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): fs2.Stream[IO, (F, V)] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * Iterates over all SSCAN members until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): fs2.Stream[IO, V] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * Iterates over all ZSCAN member/score pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): fs2.Stream[IO, (V, Double)] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // convert pages from the shared Paged helper into individual fs2 Stream elements
  private def paged[S, A](init: S)(step: Paged.Step[S, A]): fs2.Stream[IO, A] =
    CStream.unfold[S, Vector[A]](init)(step).flatMap(items => CStream.init(items)).lower

  private def scanStream[A](fetch: ScanCursor => IO[ScanPage[A]]): fs2.Stream[IO, A] =
    paged[Option[ScanCursor], A](Some(ScanCursor.start))(Paged.byCursor(cursor => CIO.lift(fetch(cursor))))

  // scan each target in sequence with its own node-local cursor. A cluster scan visits every master that owns slots.
  private def scanStreamAll[A](fetch: ScanTarget => ScanCursor => IO[ScanPage[A]]): fs2.Stream[IO, A] =
    paged[ScanStep, A](ScanStep.Begin)(Paged.acrossTargets(CIO.lift(client.scanTargets))(target => cursor => CIO.lift(fetch(target)(cursor))))

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): fs2.Stream[IO, StreamEntry[F, V]] =
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
  ): fs2.Stream[IO, StreamEntry[F, V]] =
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
  ): fs2.Stream[IO, StreamEntry[F, V]] =
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
  )(handle: StreamEntry[F, V] => IO[Unit]): IO[Unit] =
    consumeStream[F, V](group, consumer, key, count, block)
      .evalMap(entry => handle(entry) >> client.run(Streams.xAck(key, group)(entry.id)).void)
      .compile
      .drain

  private def consumeStream[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  ): fs2.Stream[IO, StreamEntry[F, V]] =
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
    * Subscribes to one or more channels. Closing the stream's scope unsubscribes. Sage resubscribes after reconnecting, but messages
    * published while the connection is down are lost.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): fs2.Stream[IO, Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): fs2.Stream[IO, PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more shard channels. In a cluster, each subscription follows its slot to the current owning node after a
    * migration or failover. Sharded deliveries use the ordinary [[Message]] type.
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): fs2.Stream[IO, Message[V]] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

  /**
    * Like [[subscribe]], but opens the subscription during resource acquisition. Standalone and master-replica clients wait for server
    * confirmation. Cluster clients wait up to the connection timeout and may return before confirmation, which can arrive later. Releasing the `Resource`
    * unsubscribes.
    */
  def subscribeResource[V: ValueCodec](channel: String, rest: String*): Resource[IO, fs2.Stream[IO, Message[V]]] =
    resourceOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[pSubscribe]], but opens the subscription during resource acquisition. Confirmation follows the same timeout behavior as
    * [[subscribeResource]]. Releasing the `Resource` unsubscribes.
    */
  def pSubscribeResource[V: ValueCodec](pattern: String, rest: String*): Resource[IO, fs2.Stream[IO, PatternMessage[V]]] =
    resourceOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[sSubscribe]], but opens the subscription during resource acquisition. Confirmation follows the same timeout behavior as
    * [[subscribeResource]]. Releasing the `Resource` unsubscribes.
    */
  def sSubscribeResource[V: ValueCodec](channel: String, rest: String*): Resource[IO, fs2.Stream[IO, Message[V]]] =
    resourceOf(client.subscribeShardChannels[V](channel, rest*))

  private def streamOf[A](open: IO[Subscription[IO, A]]): fs2.Stream[IO, A] =
    fs2.Stream.resource(resourceOf(open)).flatten

  private def resourceOf[A](open: IO[Subscription[IO, A]]): Resource[IO, fs2.Stream[IO, A]] =
    Resource.make(open)(_.close.voidError).map(sub => fs2.Stream.repeatEval(sub.next).unNoneTerminate)
}

object SageClient {

  /**
    * A client that uses `K` for keys, returned by `client.as[K]`. [[SageClient]] uses `String` keys by default. Calling `as` changes only
    * the key type and continues to use the same connection.
    */
  type Keyed[K] = Client[IO, K]
  def connect(config: SageConfig): IO[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  def resource(config: SageConfig): Resource[IO, SageClient] =
    Resource.make(connect(config))(_.close.voidError)

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[IO](underlying) {
    protected def lower[A](c: CIO[A]): IO[A] = c.lower
    protected def lift[A](fa: IO[A]): CIO[A] = CIO.lift(fa)
  }
}
