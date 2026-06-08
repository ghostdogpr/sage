package sage.kyo

import _root_.kyo.{<, Abort, Async, Scope, Stream, Tag}
import _root_.kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, RedisType, ScanCursor, Sets, SortedSets}

/**
  * The Kyo-native surface: the same client, with every method returning a Kyo pending computation.
  */
type SageClient = Client[[A] =>> A < (Abort[Throwable] & Async)]

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
}

object SageClient {

  def connect(config: SageConfig): SageClient < (Abort[Throwable] & Async) =
    Client.connect(config).lower.map(new Lowered(_))

  def scoped(config: SageConfig): SageClient < (Scope & Abort[Throwable] & Async) =
    Scope.acquireRelease(connect(config))(client => Abort.run(client.close))

  final private class Lowered(underlying: Client[CIO]) extends Client[[A] =>> A < (Abort[Throwable] & Async)] {

    def run[A](command: Command[A]): A < (Abort[Throwable] & Async) = underlying.run(command).lower

    def close: Unit < (Abort[Throwable] & Async) = underlying.close.lower
  }
}
