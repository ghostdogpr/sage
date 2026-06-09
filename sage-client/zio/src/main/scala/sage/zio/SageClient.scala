package sage.zio

import kyo.compat.*
import zio.*
import zio.stream.ZStream

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, Sets, SortedSets}

/**
  * The ZIO-native surface: the same client, with every method returning `Task`.
  */
type SageClient = Client[Task]

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): ZStream[Any, Throwable, K] =
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
  ): ZStream[Any, Throwable, (F, V)] =
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
  ): ZStream[Any, Throwable, V] =
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
  ): ZStream[Any, Throwable, (V, Double)] =
    CStream
      .unfold[Option[ScanCursor], Vector[(V, Double)]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(SortedSets.zScan[K, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(pairs => CStream.init(pairs))
      .lower

  /**
    * Subscribes to one or more channels; closing the stream's scope unsubscribes. Survives reconnects via auto-resubscribe, dropping
    * messages published during the reconnect gap.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): ZStream[Any, Throwable, Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): ZStream[Any, Throwable, PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  private def streamOf[A](open: Task[Subscription[Task, A]]): ZStream[Any, Throwable, A] =
    ZStream.unwrapScoped(
      ZIO.acquireRelease(open)(_.close.ignore).map { sub =>
        ZStream.repeatZIOOption(sub.next.mapError(Some(_)).flatMap {
          case Some(a) => ZIO.succeed(a)
          case None    => ZIO.fail(None)
        })
      }
    )
}

object SageClient {

  def connect(config: SageConfig): Task[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  def scoped(config: SageConfig): ZIO[Scope, Throwable, SageClient] =
    ZIO.acquireRelease(connect(config))(_.close.ignore)

  def layer(config: SageConfig): ZLayer[Any, Throwable, SageClient] =
    ZLayer.scoped(scoped(config))

  final private class Lowered(underlying: Client[CIO]) extends Client[Task] {

    def run[A](command: Command[A]): Task[A] = underlying.run(command).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): Task[Out] = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): Task[R] = underlying.pipelineAttempt(p).lower

    def transaction[A](body: TransactionScope[Task] => Task[A]): Task[A] =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): Task[Subscription[Task, Message[V]]] =
      underlying.subscribeChannels[V](channel, rest*).map(lower).lower

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): Task[Subscription[Task, PatternMessage[V]]] =
      underlying.subscribePatterns[V](pattern, rest*).map(lower).lower

    private def lower(scope: TransactionScope[CIO]): TransactionScope[Task] =
      new TransactionScope[Task] {
        def watch[K: KeyCodec](key: K, rest: K*): Task[Unit]          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): Task[A]                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): Task[Option[Out]]      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): Task[Option[R]] = scope.execAttempt(p).lower
        def discard: Task[Unit]                                       = scope.discard.lower
      }

    private def lower[A](sub: Subscription[CIO, A]): Subscription[Task, A] =
      new Subscription[Task, A] {
        def next: Task[Option[A]] = sub.next.lower
        def close: Task[Unit]     = sub.close.lower
      }

    def close: Task[Unit] = underlying.close.lower
  }
}
