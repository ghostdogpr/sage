package sage.kyo

import _root_.kyo.{<, Abort, Async, Scope}
import _root_.kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.Command

/**
  * The Kyo-native surface: the same client, with every method returning a Kyo pending computation.
  */
type SageClient = Client[[A] =>> A < (Abort[Throwable] & Async)]

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
