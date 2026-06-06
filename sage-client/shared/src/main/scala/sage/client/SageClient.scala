package sage.client

import kyo.compat.*

import sage.SageException.{ServerError, UnsupportedServer}
import sage.client.internal.{Multiplexer, SocketTransport}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Connection, Strings}

/**
  * The user-facing handle owning all connections to one server. Per-command methods are concrete sugar delegating to [[run]]: a fake
  * implementing `run` gets the whole command surface.
  */
trait SageClient {

  def run[A](command: Command[A]): CIO[A]

  def close: CIO[Unit]

  final def ping(message: Option[String] = None): CIO[String] = run(Connection.ping(message))

  final def get[K: KeyCodec, V: ValueCodec](key: K): CIO[Option[V]] = run(Strings.get(key))

  final def set[K: KeyCodec, V: ValueCodec](key: K, value: V): CIO[Unit] = run(Strings.set(key, value))
}

object SageClient {

  def connect(config: SageConfig): CIO[SageClient] =
    connectWith((onFrame, onClosed) => SocketTransport.connect(config.host, config.port, config.connectTimeout, onFrame, onClosed))

  private[client] def connectWith(factory: Multiplexer.TransportFactory): CIO[SageClient] =
    CIO.blocking(new Live(new Multiplexer(factory))).flatMap { client =>
      client.run(Connection.hello()).map(_ => client: SageClient).recover { error =>
        client.close.flatMap(_ => CIO.fail(translateHandshake(error)))
      }
    }

  // pre-6.0 Redis answers HELLO with an unknown-command error; newer servers reject an unsupported protocol version with NOPROTO
  private def translateHandshake(error: Throwable): Throwable =
    error match {
      case ServerError(message) if message.startsWith("NOPROTO") || message.toLowerCase.contains("unknown command") =>
        UnsupportedServer(s"sage requires RESP3 (Redis 6.0+ or any Valkey); server rejected HELLO 3: $message")
      case other                                                                                                    => other
    }

  final private class Live(multiplexer: Multiplexer) extends SageClient {

    def run[A](command: Command[A]): CIO[A] = CIO.async(callback => multiplexer.submit(command, callback))

    def close: CIO[Unit] = CIO.blocking(multiplexer.close())
  }
}
