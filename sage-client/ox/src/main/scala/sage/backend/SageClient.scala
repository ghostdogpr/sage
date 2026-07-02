package sage.backend

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

import _root_.ox.{useInScope, Ox}
import _root_.ox.flow.Flow
import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, ScanStep, ScanTarget, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * The Ox-native surface: direct style, every method usable inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A, String]

extension [K](client: Client[[A] =>> Ox ?=> A, K])(using @unused ev: KeyCodec[K]) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once. In cluster
    * mode it walks every slot-owning master in turn, each with its own node-local cursor, so the sweep covers the whole keyspace.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Ox ?=> Flow[K] =
    scanStreamAll(target => cursor => client.runOn(target, Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(F, V)] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[V] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(V, Double)] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // drives a shared Paged step machine as a Flow, flattening each page into individual elements
  private def paged[S, A](init: S)(step: Paged.Step[S, A]): Ox ?=> Flow[A] =
    CStream.unfold[S, Vector[A]](init)(step).flatMap(items => CStream.init(items)).lower

  private def scanStream[A](fetch: ScanCursor => (Ox ?=> ScanPage[A])): Ox ?=> Flow[A] =
    paged[Option[ScanCursor], A](Some(ScanCursor.start))(Paged.byCursor(cursor => CIO.lift(fetch(cursor))))

  // walks every scan target in turn, each with its own node-local cursor, so a cluster SCAN sweeps all masters instead of one
  private def scanStreamAll[A](fetch: ScanTarget => ScanCursor => (Ox ?=> ScanPage[A])): Ox ?=> Flow[A] =
    paged[ScanStep, A](ScanStep.Begin)(Paged.acrossTargets(CIO.lift(client.scanTargets))(target => cursor => CIO.lift(fetch(target)(cursor))))

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): Ox ?=> Flow[StreamEntry[F, V]] =
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
  ): Ox ?=> Flow[StreamEntry[F, V]] =
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
    block: BlockTimeout = Paged.defaultPoll
  ): Ox ?=> Flow[StreamEntry[F, V]] =
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
    block: BlockTimeout = Paged.defaultPoll
  )(handle: StreamEntry[F, V] => (Ox ?=> Unit)): Ox ?=> Unit =
    consumeStream[F, V](group, consumer, key, count, block).runForeach { entry =>
      handle(entry)
      client.run(Streams.xAck(key, group)(entry.id))
      ()
    }

  private def consumeStream[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  ): Ox ?=> Flow[StreamEntry[F, V]] =
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
    * Subscribes to one or more channels, returning once the server has confirmed the SUBSCRIBE so a publish issued after this call cannot
    * race the registration. Ending the flow unsubscribes. Survives reconnects via auto-resubscribe, dropping messages published during the
    * reconnect gap.
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

  // subscribe eagerly so the SUBSCRIBE is confirmed before returning; unsubscribe once, when the flow ends or (if never run) at scope close
  private def streamOf[A](open: => Subscription[[X] =>> Ox ?=> X, A]): Ox ?=> Flow[A] = {
    val sub                 = open
    val closed              = new AtomicBoolean(false)
    def unsubscribe(): Unit = if (closed.compareAndSet(false, true)) sub.close
    val _                   = useInScope(sub)(_ => unsubscribe())
    Flow.usingEmit { emit =>
      try {
        var continue = true
        while (continue)
          sub.next match {
            case Some(a) => emit(a)
            case None    => continue = false
          }
      } finally unsubscribe()
    }
  }
}

object SageClient {

  /**
    * A command surface re-typed to a non-String key, as returned by `client.as[K]`. The unqualified [[SageClient]] is String-keyed;
    * use `as` to reach any other key type over the same connection.
    */
  type Keyed[K] = Client[[A] =>> Ox ?=> A, K]
  def connect(config: SageConfig): Ox ?=> SageClient = new Lowered(Client.connect(config).lower)

  def scoped(config: SageConfig): Ox ?=> SageClient =
    useInScope(connect(config)) { client =>
      // never fail teardown: swallow unconditionally (incl. InterruptedException), matching the zio/ce/kyo close-on-release policy
      try client.close
      catch { case _: Throwable => () }
    }

  final private class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[X] =>> Ox ?=> X](underlying) {
    protected def lower[A](c: CIO[A]): Ox ?=> A = c.lower
    protected def lift[A](fa: Ox ?=> A): CIO[A] = CIO.lift(fa)
  }
}
