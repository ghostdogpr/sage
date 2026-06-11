package sage.ox

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import _root_.ox.{useInScope, Ox}
import _root_.ox.flow.Flow
import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, ScanStep, ScanTarget, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * The Ox-native surface: direct style, every method usable inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A]

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once. In cluster
    * mode it walks every slot-owning master in turn, each with its own node-local cursor, so the sweep covers the whole keyspace.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Ox ?=> Flow[K] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(F, V)] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[V] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(V, Double)] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  private def scanStream[A](fetch: ScanCursor => (Ox ?=> ScanPage[A])): Ox ?=> Flow[A] =
    CStream
      .unfold[Option[ScanCursor], Vector[A]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) => CIO.lift(fetch(cursor)).map(page => Some((page.items, page.next)))
      }
      .flatMap(items => CStream.init(items))
      .lower

  // walks every scan target in turn, each with its own node-local cursor, so a cluster SCAN sweeps all masters instead of one
  private def scanStreamAll[A](fetch: ScanTarget => ScanCursor => (Ox ?=> ScanPage[A])): Ox ?=> Flow[A] =
    CStream
      .unfold[ScanStep, Vector[A]](ScanStep.Begin) {
        case ScanStep.Begin                 =>
          CIO
            .lift(client.scanTargets)
            .map(targets => if (targets.isEmpty) None else Some((Vector.empty[A], ScanStep.Visit(ScanCursor.start, targets))))
        case ScanStep.Visit(cursor, remain) =>
          CIO.lift(fetch(remain.head)(cursor)).map { page =>
            page.next match {
              case Some(next) => Some((page.items, ScanStep.Visit(next, remain)))
              case None       => Some((page.items, if (remain.tail.isEmpty) ScanStep.End else ScanStep.Visit(ScanCursor.start, remain.tail)))
            }
          }
        case ScanStep.End                   => CIO.value(None)
      }
      .flatMap(items => CStream.init(items))
      .lower

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): Ox ?=> Flow[StreamEntry[F, V]] =
    CStream
      .unfold[Option[StreamRangeId], Vector[StreamEntry[F, V]]](Some(start)) {
        case None       => CIO.value(None)
        case Some(from) =>
          CIO.lift(client.run(Streams.xRange[K, F, V](key, from, end, Some(batch)))).map { entries =>
            if (entries.isEmpty) None
            else Some((entries, if (entries.length < batch) None else Some(StreamRangeId.Exclusive(entries.last.id))))
          }
      }
      .flatMap(items => CStream.init(items))
      .lower

  /**
    * Tails a consumer group: first drains this consumer's own pending history (at-least-once recovery after a restart), then blocks for new
    * entries forever. `handle` runs per entry; the entry is acknowledged only after `handle` returns, so a failure leaves it in the PEL for
    * recovery. See ADR-0032.
    */
  def xConsume[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long] = None,
    block: BlockTimeout = SageClient.defaultPoll
  )(handle: StreamEntry[F, V] => (Ox ?=> Unit)): Ox ?=> Unit =
    consumeStream[K, F, V](group, consumer, key, count, block).runForeach { entry =>
      handle(entry)
      client.run(Streams.xAck(key, group)(entry.id))
      ()
    }

  private def consumeStream[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  ): Ox ?=> Flow[StreamEntry[F, V]] =
    CStream
      .unfold[Either[StreamId, Unit], Vector[StreamEntry[F, V]]](Left(StreamId.Zero)) {
        case Left(after) =>
          CIO.lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.After(after)))(count = count))).map { result =>
            val entries = result.flatMap(_._2)
            if (entries.isEmpty) Some((Vector.empty, Right(()))) else Some((entries, Left(entries.last.id)))
          }
        case Right(_)    =>
          CIO.lift(client.run(Streams.xReadGroup[K, F, V](group, consumer)((key, GroupReadId.New))(count = count, block = Some(block)))).map { result =>
            Some((result.flatMap(_._2), Right(())))
          }
      }
      .flatMap(items => CStream.init(items))
      .lower

  /**
    * Subscribes to one or more channels; ending the flow unsubscribes. Survives reconnects via auto-resubscribe, dropping messages
    * published during the reconnect gap.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): Ox ?=> Flow[PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more Shard Channels; in a cluster each is routed to the Node owning its Slot, and resubscription follows the Slot
    * on migration or failover. A sharded delivery is an ordinary [[Message]].
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): Ox ?=> Flow[Message[V]] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

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
}

object SageClient {

  // bounded poll so xConsume's blocking read returns periodically, keeping cancellation responsive
  private[ox] val defaultPoll: BlockTimeout = BlockTimeout.After(FiniteDuration(5, TimeUnit.SECONDS))

  def connect(config: SageConfig): Ox ?=> SageClient = new Lowered(Client.connect(config).lower)

  def scoped(config: SageConfig): Ox ?=> SageClient =
    useInScope(connect(config)) { client =>
      // never fail teardown: swallow unconditionally (incl. InterruptedException), matching the zio/ce/kyo close-on-release policy
      try client.close
      catch { case _: Throwable => () }
    }

  final private class Lowered(underlying: Client[CIO]) extends Client[[A] =>> Ox ?=> A] {

    def run[A](command: Command[A]): Ox ?=> A = underlying.run(command).lower

    def cached[A](command: Command[A], ttl: FiniteDuration): Ox ?=> A = underlying.cached(command, ttl).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): Ox ?=> Out = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): Ox ?=> R = underlying.pipelineAttempt(p).lower

    def transaction[A](body: TransactionScope[[X] =>> Ox ?=> X] => (Ox ?=> A)): Ox ?=> A =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): Ox ?=> Subscription[[X] =>> Ox ?=> X, Message[V]] =
      lower(underlying.subscribeChannels[V](channel, rest*).lower)

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): Ox ?=> Subscription[[X] =>> Ox ?=> X, PatternMessage[V]] =
      lower(underlying.subscribePatterns[V](pattern, rest*).lower)

    def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): Ox ?=> Subscription[[X] =>> Ox ?=> X, Message[V]] =
      lower(underlying.subscribeShardChannels[V](channel, rest*).lower)

    def scanTargets: Ox ?=> Vector[ScanTarget] = underlying.scanTargets.lower

    def runOn[A](target: ScanTarget, command: Command[A]): Ox ?=> A = underlying.runOn(target, command).lower

    private def lower(scope: TransactionScope[CIO]): TransactionScope[[X] =>> Ox ?=> X] =
      new TransactionScope[[X] =>> Ox ?=> X] {
        def watch[K: KeyCodec](key: K, rest: K*): Ox ?=> Unit          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): Ox ?=> A                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): Ox ?=> Option[Out]      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): Ox ?=> Option[R] = scope.execAttempt(p).lower
        def discard: Ox ?=> Unit                                       = scope.discard.lower
      }

    private def lower[A](sub: Subscription[CIO, A]): Subscription[[X] =>> Ox ?=> X, A] =
      new Subscription[[X] =>> Ox ?=> X, A] {
        def next: Ox ?=> Option[A] = sub.next.lower
        def close: Ox ?=> Unit     = sub.close.lower
      }

    def close: Ox ?=> Unit = underlying.close.lower
  }
}
