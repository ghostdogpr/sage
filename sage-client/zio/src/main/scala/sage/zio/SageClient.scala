package sage.zio

import kyo.compat.*
import zio.*
import zio.stream.ZStream

import sage.client.SageConfig
import sage.client.internal.Client
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, RedisType, ScanCursor}

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

    def close: Task[Unit] = underlying.close.lower
  }
}
