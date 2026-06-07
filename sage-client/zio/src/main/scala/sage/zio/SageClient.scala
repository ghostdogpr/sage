package sage.zio

import kyo.compat.*
import zio.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.Command

/**
  * The ZIO-native surface: the same client, with every method returning `Task`.
  */
type SageClient = Client[Task]

object SageClient {

  def connect(config: SageConfig): Task[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  final private class Lowered(underlying: Client[CIO]) extends Client[Task] {

    def run[A](command: Command[A]): Task[A] = underlying.run(command).lower

    def close: Task[Unit] = underlying.close.lower
  }
}
