package sage.ce

import cats.effect.{IO, Resource}
import kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.Command

/**
  * The cats-effect-native surface: the same client, with every method returning `IO`.
  */
type SageClient = Client[IO]

object SageClient {

  def connect(config: SageConfig): IO[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  def resource(config: SageConfig): Resource[IO, SageClient] =
    Resource.make(connect(config))(_.close.voidError)

  final private class Lowered(underlying: Client[CIO]) extends Client[IO] {

    def run[A](command: Command[A]): IO[A] = underlying.run(command).lower

    def close: IO[Unit] = underlying.close.lower
  }
}
