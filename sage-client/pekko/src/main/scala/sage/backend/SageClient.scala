package sage.backend

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

import kyo.compat.*
import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.{KillSwitches, Materializer, SystemMaterializer, UniqueKillSwitch}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import sage.{Message, PatternMessage, SageException}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, ScanStep, ScanTarget, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * A Sage client for Pekko. Methods return `scala.concurrent.Future`, and streaming methods return a Pekko Streams `Source`.
  */
type SageClient = Client[Future, String]

extension [K](client: Client[Future, K])(using @unused ev: KeyCodec[K]) {

  /**
    * Scans the full keyspace. An empty page does not end the scan; iteration stops when the server returns a zero cursor. Redis may return
    * the same key more than once. In cluster mode, each master is scanned with its own cursor.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Source[K, NotUsed] =
    scanSourceAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * Iterates over all HSCAN field/value pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Source[(F, V), NotUsed] =
    scanSource(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * Iterates over all SSCAN members until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Source[V, NotUsed] =
    scanSource(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * Iterates over all ZSCAN member/score pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Source[(V, Double), NotUsed] =
    scanSource(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): Source[StreamEntry[F, V], NotUsed] =
    pagedSource[Option[StreamRangeId], StreamEntry[F, V]](Some(start))(
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
  ): Source[StreamEntry[F, V], NotUsed] =
    pagedSource[Option[StreamId], StreamEntry[F, V]](Some(start))(
      Paged.byAutoClaim(from => CIO.lift(client.run(Streams.xAutoClaim[K, F, V](key, group, consumer, minIdle, from, count))))
    )

  /**
    * Follows a stream without a consumer group: replays every entry after `from`, then blocks for new entries forever. Cancel the `Source`
    * to stop tailing.
    */
  def xTail[F: KeyCodec, V: ValueCodec](
    key: K,
    from: StreamId = StreamId.Zero,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
  ): Source[StreamEntry[F, V], NotUsed] =
    SageClient.boundedPoll(block, "xTail") match {
      case Left(e)     => Source.failed(e)
      case Right(poll) =>
        pagedSource[StreamId, StreamEntry[F, V]](from)(
          Paged.tail(last =>
            CIO
              .lift(client.run(Streams.xRead[K, F, V]((key, ReadId.After(last)))(count = count, block = Some(poll))))
              .map(_.flatMap(_._2))
          )
        )
    }

  /**
    * Follows a stream as part of a consumer group. It processes this consumer's pending entries first, then waits for new entries. Each entry
    * is acknowledged only after `handle` succeeds. A `Future` cannot be cancelled, so this method returns a [[RunningConsumer]]. Call `stop()`
    * to stop between entries and wait for `completion`.
    */
  def xConsume[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
  )(handle: StreamEntry[F, V] => Future[Unit])(using system: ActorSystem[?]): RunningConsumer =
    SageClient.boundedPoll(block, "xConsume") match {
      case Left(e)     => RunningConsumer.failed(e)
      case Right(poll) =>
        given Materializer     = SystemMaterializer(system).materializer
        given ExecutionContext = system.executionContext
        val (killSwitch, done) = consumeSource[F, V](group, consumer, key, count, poll)
          .viaMat(KillSwitches.single)(Keep.right)
          .mapAsync(1)(entry => handle(entry).flatMap(_ => client.run(Streams.xAck(key, group)(entry.id)).map(_ => ())))
          .toMat(Sink.ignore)(Keep.both)
          .run()
        RunningConsumer(killSwitch, done)
    }

  /**
    * Subscribes to one or more channels; the subscription opens when the `Source` is materialized and unsubscribes when the stream is
    * cancelled, completes, or fails. The materialized value is a `Future[Done]` that completes once the subscription is registered, so a
    * publish sequenced after it does not race the registration on a standalone or master-replica server. In cluster mode that confirmation is
    * best-effort (it may resolve before every owning node has acked), matching the shared runtime's subscribe semantics across all backends.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): Source[Message[V], Future[Done]] =
    subscriptionSource(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): Source[PatternMessage[V], Future[Done]] =
    subscriptionSource(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more Shard Channels; in a cluster each is routed to the Node owning its Slot, and resubscription follows the Slot
    * on migration or failover. A sharded delivery is an ordinary [[Message]].
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): Source[Message[V], Future[Done]] =
    subscriptionSource(client.subscribeShardChannels[V](channel, rest*))

  private def scanSource[A](fetch: ScanCursor => Future[ScanPage[A]]): Source[A, NotUsed] =
    pagedSource[Option[ScanCursor], A](Some(ScanCursor.start))(Paged.byCursor(cursor => CIO.lift(fetch(cursor))))

  // scan each target with its own node-local cursor. A cluster has one target for every slot-owning master.
  private def scanSourceAll[A](fetch: ScanTarget => ScanCursor => Future[ScanPage[A]]): Source[A, NotUsed] =
    pagedSource[ScanStep, A](ScanStep.Begin)(
      Paged.acrossTargets(CIO.lift(client.scanTargets))(target => cursor => CIO.lift(fetch(target)(cursor)))
    )

  private def consumeSource[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  ): Source[StreamEntry[F, V], NotUsed] =
    pagedSource[Either[StreamId, Unit], StreamEntry[F, V]](Left(StreamId.Zero))(
      Paged.consume(
        drainPending = after =>
          CIO.lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.After(after)))(count = count))).map(_.flatMap(_._2)),
        tailNew = CIO
          .lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.New))(count = count, block = Some(block))))
          .map(_.flatMap(_._2))
      )
    )

  // convert pages from the shared Paged helper into individual Source elements
  private def pagedSource[S, A](init: S)(step: Paged.Step[S, A]): Source[A, NotUsed] =
    Source
      .unfoldAsync[S, Vector[A]](init)(s => step(s).unsafeRun.map(_.map { case (items, next) => (next, items) })(ExecutionContext.parasitic))
      .mapConcat(identity)

  // Open on materialization and close on cancellation, completion, or failure. Complete Future[Done] after the subscription open call finishes;
  // cluster confirmation follows the best-effort behavior described above.
  private def subscriptionSource[A](open: => Future[Subscription[Future, A]]): Source[A, Future[Done]] =
    Source
      .fromMaterializer { (_, _) =>
        val confirmed = Promise[Done]()
        Source
          .unfoldResourceAsync[A, Subscription[Future, A]](
            () => {
              val opened = open
              opened.onComplete {
                case Success(_) => confirmed.trySuccess(Done)
                case Failure(e) => confirmed.tryFailure(e)
              }(ExecutionContext.parasitic)
              opened
            },
            sub => sub.next,
            // swallow unsubscribe errors on teardown (close-on-release policy)
            sub => sub.close.map(_ => Done)(ExecutionContext.parasitic).recover { case _ => Done }(ExecutionContext.parasitic)
          )
          .mapMaterializedValue(_ => confirmed.future)
      }
      .mapMaterializedValue(_.flatten)
}

object SageClient {

  /**
    * A client that uses `K` for keys, returned by `client.as[K]`. [[SageClient]] uses `String` keys by default. Calling `as` changes only
    * the key type and continues to use the same connection.
    */
  type Keyed[K] = Client[Future, K]

  /**
    * Connects and returns a Pekko-native client. The caller owns the client lifecycle and must call `close` explicitly.
    */
  def connect(config: SageConfig): Future[SageClient] =
    Client.connect(config).unsafeRun.map(new Lowered(_))(ExecutionContext.parasitic)

  /**
    * Connects, runs `f`, and then closes the client whether `f` succeeds, throws, or returns a failed `Future`. Errors raised while closing
    * are ignored, as they are by the other backends' `scoped` and `resource` helpers. Prefer this to managing `connect` and `close`
    * yourself unless the client must outlive a single operation.
    */
  def use[A](config: SageConfig)(f: SageClient => Future[A])(using ExecutionContext): Future[A] =
    connect(config).flatMap(client => useConnected(client)(f))

  private[backend] def useConnected[A](client: SageClient)(f: SageClient => Future[A])(using ExecutionContext): Future[A] = {
    val ran =
      try f(client)
      catch { case e: Throwable => Future.failed(e) }
    ran.transformWith(result => client.close.transformWith(_ => Future.fromTry(result)))
  }

  private[backend] def boundedPoll(block: BlockTimeout, method: String): Either[SageException.InvalidArgument, BlockTimeout] =
    block match {
      case BlockTimeout.Forever =>
        Left(
          SageException.InvalidArgument(
            s"$method cannot use BlockTimeout.Forever on the Pekko backend; Future cannot interrupt an in-flight blocking read"
          )
        )
      case other                => Right(other)
    }

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[Future](underlying) {
    protected def lower[A](c: CIO[A]): Future[A] = c.unsafeRun
    protected def lift[A](fa: Future[A]): CIO[A] = CIO.lift(fa)
  }
}

/**
  * A handle to a running [[xConsume]] loop. Because `Future` cannot be interrupted, a Pekko `KillSwitch` stops the loop between entries.
  * `completion` succeeds when the loop stops and fails if handling or acknowledging an entry failed.
  */
final class RunningConsumer private[backend] (killSwitch: Option[UniqueKillSwitch], val completion: Future[Done]) {

  /**
    * Requests a graceful stop between entries and returns [[completion]] (which fails if the loop had already failed in `handle`).
    */
  def stop(): Future[Done] = {
    killSwitch.foreach(_.shutdown())
    completion
  }
}

object RunningConsumer {

  private[backend] def apply(killSwitch: UniqueKillSwitch, completion: Future[Done]): RunningConsumer =
    new RunningConsumer(Some(killSwitch), completion)

  private[backend] def failed(error: Throwable): RunningConsumer =
    new RunningConsumer(None, Future.failed(error))
}
