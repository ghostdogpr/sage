package sage.ox

import scala.util.control.NonFatal

import _root_.ox.{useInScope, Ox}
import _root_.ox.flow.Flow
import kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.codec.KeyCodec
import sage.commands.{Command, Keys, RedisType, ScanCursor}

/**
  * The Ox-native surface: direct style, every method usable inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A]

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Ox ?=> Flow[K] =
    CStream
      .unfold[Option[ScanCursor], Vector[K]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Keys.scan[K](cursor, pattern, count, ofType))).map(page => Some((page.keys, page.next)))
      }
      .flatMap(keys => CStream.init(keys))
      .lower
}

object SageClient {

  def connect(config: SageConfig): Ox ?=> SageClient = new Lowered(Client.connect(config).lower)

  def scoped(config: SageConfig): Ox ?=> SageClient =
    useInScope(connect(config)) { client =>
      try client.close
      catch { case NonFatal(_) => () }
    }

  final private class Lowered(underlying: Client[CIO]) extends Client[[A] =>> Ox ?=> A] {

    def run[A](command: Command[A]): Ox ?=> A = underlying.run(command).lower

    def close: Ox ?=> Unit = underlying.close.lower
  }
}
