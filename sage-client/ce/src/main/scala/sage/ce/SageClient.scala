package sage.ce

import cats.effect.{IO, Resource}
import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, Sets, SortedSets}

/**
  * The cats-effect-native surface: the same client, with every method returning `IO`.
  */
type SageClient = Client[IO]

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): fs2.Stream[IO, K] =
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
  ): fs2.Stream[IO, (F, V)] =
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
  ): fs2.Stream[IO, V] =
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
  ): fs2.Stream[IO, (V, Double)] =
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
  def subscribe[V: ValueCodec](channel: String, rest: String*): fs2.Stream[IO, Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): fs2.Stream[IO, PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  private def streamOf[A](open: IO[Subscription[IO, A]]): fs2.Stream[IO, A] =
    fs2.Stream
      .resource(Resource.make(open)(_.close.voidError))
      .flatMap(sub => fs2.Stream.repeatEval(sub.next).unNoneTerminate)
}

object SageClient {

  def connect(config: SageConfig): IO[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  def resource(config: SageConfig): Resource[IO, SageClient] =
    Resource.make(connect(config))(_.close.voidError)

  final private class Lowered(underlying: Client[CIO]) extends Client[IO] {

    def run[A](command: Command[A]): IO[A] = underlying.run(command).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): IO[Out] = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): IO[R] = underlying.pipelineAttempt(p).lower

    def transaction[A](body: TransactionScope[IO] => IO[A]): IO[A] =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): IO[Subscription[IO, Message[V]]] =
      underlying.subscribeChannels[V](channel, rest*).map(lower).lower

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): IO[Subscription[IO, PatternMessage[V]]] =
      underlying.subscribePatterns[V](pattern, rest*).map(lower).lower

    private def lower(scope: TransactionScope[CIO]): TransactionScope[IO] =
      new TransactionScope[IO] {
        def watch[K: KeyCodec](key: K, rest: K*): IO[Unit]          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): IO[A]                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): IO[Option[Out]]      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): IO[Option[R]] = scope.execAttempt(p).lower
        def discard: IO[Unit]                                       = scope.discard.lower
      }

    private def lower[A](sub: Subscription[CIO, A]): Subscription[IO, A] =
      new Subscription[IO, A] {
        def next: IO[Option[A]] = sub.next.lower
        def close: IO[Unit]     = sub.close.lower
      }

    def close: IO[Unit] = underlying.close.lower
  }
}
