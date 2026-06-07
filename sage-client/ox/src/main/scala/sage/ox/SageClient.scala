package sage.ox

import scala.util.control.NonFatal

import _root_.ox.{useInScope, Ox}
import kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.Command

/**
  * The Ox-native surface: direct style, every method usable inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A]

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
