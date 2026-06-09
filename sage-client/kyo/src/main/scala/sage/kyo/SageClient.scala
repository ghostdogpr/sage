package sage.kyo

import _root_.kyo.{<, Abort, Async, Frame, Maybe, Scope, Stream, Tag}
import _root_.kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, Sets, SortedSets}

/**
  * The Kyo-native surface: the same client, with every method returning a Kyo pending computation.
  */
type SageClient = Client[[A] =>> A < (Abort[Throwable] & Async)]

private type KyoEff[A] = A < (Abort[Throwable] & Async)

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using Tag[K]): Stream[K, Abort[Throwable] & Async] =
    CStream
      .unfold[Option[ScanCursor], Vector[K]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Keys.scan[K](cursor, pattern, count, ofType))).map(page => Some((page.items, page.next)))
      }
      .flatMap(keys => CStream.init(keys))
      .lower

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[F], Tag[V]): Stream[(F, V), Abort[Throwable] & Async] =
    CStream
      .unfold[Option[ScanCursor], Vector[(F, V)]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(pairs => CStream.init(pairs))
      .lower

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[V, Abort[Throwable] & Async] =
    CStream
      .unfold[Option[ScanCursor], Vector[V]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Sets.sScan[K, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(members => CStream.init(members))
      .lower

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  )(using Tag[V]): Stream[(V, Double), Abort[Throwable] & Async] =
    CStream
      .unfold[Option[ScanCursor], Vector[(V, Double)]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(SortedSets.zScan[K, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(pairs => CStream.init(pairs))
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
