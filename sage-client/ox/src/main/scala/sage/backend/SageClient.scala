package sage.backend

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

import _root_.ox.{useInScope, Ox}
import _root_.ox.flow.Flow
import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * A direct-style Sage client for use inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A, String]

extension [K](client: Client[[A] =>> Ox ?=> A, K])(using @unused ev: KeyCodec[K]) {

  /**
    * Scans the full keyspace. An empty page does not end the scan; iteration stops when the server returns a zero cursor. Redis may return
    * the same key more than once. In cluster mode, each master is scanned with its own cursor.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Ox ?=> Flow[K] =
    paged(Paged.scanAll[K](client.runner, pattern, count, ofType))

  /**
    * Iterates over all HSCAN field/value pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(F, V)] =
    paged(Paged.scanKey(client.runner)(cursor => Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * Iterates over all SSCAN members until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[V] =
    paged(Paged.scanKey(client.runner)(cursor => Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * Iterates over all ZSCAN member/score pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(V, Double)] =
    paged(Paged.scanKey(client.runner)(cursor => SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // convert pages from the shared Paged helper into individual Flow elements
  private def paged[S, A](pages: Paged.Pages[S, A]): Ox ?=> Flow[A] =
    CStream.unfold[S, Vector[A]](pages.init)(pages.step).flatMap(items => CStream.init(items)).lower

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): Ox ?=> Flow[StreamEntry[F, V]] =
    paged(Paged.xRangeAll[K, F, V](client.runner, key, start, end, batch))

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
  ): Ox ?=> Flow[StreamEntry[F, V]] =
    paged(Paged.xAutoClaimAll[K, F, V](client.runner, key, group, consumer, minIdle, start, count))

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
  ): Ox ?=> Flow[StreamEntry[F, V]] =
    paged(Paged.xTail[K, F, V](client.runner, key, from, count, block))

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
  )(handle: StreamEntry[F, V] => (Ox ?=> Unit)): Ox ?=> Unit =
    paged(Paged.xConsume[K, F, V](client.runner, group, consumer, key, count, block)).runForeach { entry =>
      handle(entry)
      client.run(Streams.xAck(key, group)(entry.id))
      ()
    }

  /**
    * Subscribes to one or more channels each time the returned `Flow` runs. Ending the flow unsubscribes. Sage resubscribes after
    * reconnecting, but messages published while the connection is down are lost. With standalone and master-replica clients, use
    * [[subscribeScoped]] when publishing must wait for the server to confirm the subscription. Cluster clients may return before confirmation.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): Ox ?=> Flow[PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more shard channels. In a cluster, each subscription follows its slot to the current owning node after a
    * migration or failover. Sharded deliveries use the ordinary [[Message]] type.
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

  /**
    * Like [[subscribe]], but opens the subscription before returning. Standalone and master-replica clients wait for server confirmation.
    * Cluster clients wait up to the connection timeout and may return before confirmation, which can arrive later. Closing the enclosing Ox scope
    * unsubscribes.
    */
  def subscribeScoped[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    scopedStreamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[pSubscribe]], but opens the subscription before returning. Confirmation follows the same timeout behavior as [[subscribeScoped]].
    * Closing the enclosing Ox scope unsubscribes.
    */
  def pSubscribeScoped[V: ValueCodec](pattern: String, rest: String*): Ox ?=> Flow[PatternMessage[V]] =
    scopedStreamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[sSubscribe]], but opens the subscription before returning. Confirmation follows the same timeout behavior as [[subscribeScoped]].
    * Closing the enclosing Ox scope unsubscribes.
    */
  def sSubscribeScoped[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    scopedStreamOf(client.subscribeShardChannels[V](channel, rest*))

  // open a new subscription for each run because an Ox Flow is reusable. The previous run closes its subscription when it ends.
  private def streamOf[A](open: => Subscription[[X] =>> Ox ?=> X, A]): Ox ?=> Flow[A] =
    Flow.usingEmit { emit =>
      val sub = open
      try {
        var continue = true
        while (continue)
          sub.next match {
            case Some(a) => emit(a)
            case None    => continue = false
          }
      } finally sub.close
    }

  private def scopedStreamOf[A](open: => Subscription[[X] =>> Ox ?=> X, A]): Ox ?=> Flow[A] = {
    val sub = useInScope(open)(_.close)
    Flow.usingEmit { emit =>
      var continue = true
      while (continue)
        sub.next match {
          case Some(a) => emit(a)
          case None    => continue = false
        }
    }
  }
}

object SageClient {

  final private class LockBodyFailure(val original: scala.util.control.ControlThrowable) extends RuntimeException

  /**
    * A client that uses `K` for keys, returned by `client.as[K]`. [[SageClient]] uses `String` keys by default. Calling `as` changes only
    * the key type and continues to use the same connection.
    */
  type Keyed[K] = Client[[A] =>> Ox ?=> A, K]
  def connect(config: SageConfig): Ox ?=> SageClient = new Lowered(Client.connect(config).lower)

  def scoped(config: SageConfig): Ox ?=> SageClient =
    useInScope(connect(config)) { client =>
      // ignore close failures, including interruption, to match the release behavior of the ZIO, Cats Effect, and Kyo backends.
      try client.close
      catch { case _: Throwable => () }
    }

  final private[sage] class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[X] =>> Ox ?=> X](underlying) {
    protected def lower[A](c: CIO[A]): Ox ?=> A = c.lower
    protected def lift[A](fa: Ox ?=> A): CIO[A] = CIO.lift(fa)

    override protected def lockScope[A, B](body: () => (Ox ?=> A))(runScope: CIO[A] => CIO[B]): Ox ?=> B = {
      val work: CIO[A] = CIO.deferLift {
        try body()
        catch { case control: scala.util.control.ControlThrowable => throw new LockBodyFailure(control) }
      }
      try runScope(work).lower
      catch { case failure: LockBodyFailure => throw failure.original }
    }
  }
}
