package sage.kyo

import scala.concurrent.duration.FiniteDuration

import _root_.kyo.{<, Abort, Async, Duration, Frame, Maybe, Scope, Stream, Tag}
import _root_.kyo.Duration.toMillis
import _root_.kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, ScanPage, Sets, SortedSets}

/**
  * The Kyo-native surface: the same client, with every method returning a Kyo pending computation.
  */
type SageClient = Client[[A] =>> A < (Abort[Throwable] & Async)]

private type KyoEff[A] = A < (Abort[Throwable] & Async)

extension (client: SageClient) {

  /**
    * Runs a read with client-side caching and a Kyo `Duration` TTL — the Kyo-native form of [[Client.cached]].
    */
  def cached[A](command: Command[A], ttl: Duration): A < (Abort[Throwable] & Async) =
    client.cached(command, FiniteDuration(ttl.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using Tag[K]): Stream[K, Abort[Throwable] & Async] =
    scanStream(cursor => client.run(Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[(F, V), Abort[Throwable] & Async] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[V, Abort[Throwable] & Async] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[(V, Double), Abort[Throwable] & Async] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  private def scanStream[A](fetch: ScanCursor => KyoEff[ScanPage[A]])(using Tag[A]): Stream[A, Abort[Throwable] & Async] =
    CStream
      .unfold[Option[ScanCursor], Vector[A]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) => CIO.lift(fetch(cursor)).map(page => Some((page.items, page.next)))
      }
      .flatMap(items => CStream.init(items))
      .lower

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

  private def streamOf[A](
    open: => Subscription[KyoEff, A] < (Abort[Throwable] & Async)
  )(using Tag[A], Frame): Stream[A, Abort[Throwable] & Async & Scope] =
    // chunkSize = 1: emit each message as it arrives — the default rechunks to 4096, withholding a live stream until that many accumulate
    Stream
      .init(Scope.acquireRelease(open)(_.close).map(Seq(_)))
      .flatMap(sub => Stream.repeatPresent(sub.next.map(opt => Maybe.fromOption(opt.map(Seq(_)))), chunkSize = 1))
}

object SageClient {

  def connect(config: SageConfig): SageClient < (Abort[Throwable] & Async) =
    Client.connect(config).lower.map(new Lowered(_))

  def scoped(config: SageConfig): SageClient < (Scope & Abort[Throwable] & Async) =
    Scope.acquireRelease(connect(config))(client => Abort.run(client.close))

  final private class Lowered(underlying: Client[CIO]) extends Client[[A] =>> A < (Abort[Throwable] & Async)] {

    def run[A](command: Command[A]): A < (Abort[Throwable] & Async) = underlying.run(command).lower

    def cached[A](command: Command[A], ttl: FiniteDuration): A < (Abort[Throwable] & Async) = underlying.cached(command, ttl).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): Out < (Abort[Throwable] & Async) = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): R < (Abort[Throwable] & Async) = underlying.pipelineAttempt(p).lower

    def transaction[A](
      body: TransactionScope[[X] =>> X < (Abort[Throwable] & Async)] => A < (Abort[Throwable] & Async)
    ): A < (Abort[Throwable] & Async) =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): Subscription[KyoEff, Message[V]] < (Abort[Throwable] & Async) =
      underlying.subscribeChannels[V](channel, rest*).lower.map(lower)

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): Subscription[KyoEff, PatternMessage[V]] < (Abort[Throwable] & Async) =
      underlying.subscribePatterns[V](pattern, rest*).lower.map(lower)

    def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): Subscription[KyoEff, Message[V]] < (Abort[Throwable] & Async) =
      underlying.subscribeShardChannels[V](channel, rest*).lower.map(lower)

    private def lower(scope: TransactionScope[CIO]): TransactionScope[[X] =>> X < (Abort[Throwable] & Async)] =
      new TransactionScope[[X] =>> X < (Abort[Throwable] & Async)] {
        def watch[K: KeyCodec](key: K, rest: K*): Unit < (Abort[Throwable] & Async)          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): A < (Abort[Throwable] & Async)                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): Option[Out] < (Abort[Throwable] & Async)      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): Option[R] < (Abort[Throwable] & Async) = scope.execAttempt(p).lower
        def discard: Unit < (Abort[Throwable] & Async)                                       = scope.discard.lower
      }

    private def lower[A](sub: Subscription[CIO, A]): Subscription[KyoEff, A] =
      new Subscription[KyoEff, A] {
        def next: Option[A] < (Abort[Throwable] & Async) = sub.next.lower
        def close: Unit < (Abort[Throwable] & Async)     = sub.close.lower
      }

    def close: Unit < (Abort[Throwable] & Async) = underlying.close.lower
  }
}
