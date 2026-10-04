package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.{Failure, Try}

import sage.client.{BuildInfo, SageConfig}
import sage.commands.{Command, Connection}

private[client] object Bootstrap {

  /**
    * The setup commands every connection runs after `HELLO` and re-runs on reconnection. Standalone, cluster, and master-replica clients
    * share this list, keeping connection identification consistent across topologies. `SELECT` lives here rather than as a runtime command
    * because it would move the database under every fiber sharing the connection.
    */
  def commands(config: SageConfig): Vector[Command[?]] = {
    val identification = Vector(
      Connection.clientSetInfo("LIB-NAME", "sage"),
      Connection.clientSetInfo("LIB-VER", BuildInfo.version)
    ) ++ config.clientName.map(Connection.clientSetName).toVector
    val selectDb       = if (config.database > 0) Vector(Connection.select(config.database)) else Vector.empty
    (Connection.hello(config.auth.map(a => a.username -> a.password)) +: identification) ++ selectDb
  }

  /**
    * Submits one command and blocks the calling thread for its reply, failing with `timedOut` when none arrives in time.
    */
  def awaitReply[A](timeoutMillis: Long, timedOut: => Throwable)(submit: (Try[A] => Unit) => Unit): Try[A] = {
    val latch   = new CountDownLatch(1)
    val outcome = new AtomicReference[Try[A]]()
    submit { result =>
      outcome.set(result)
      latch.countDown()
    }
    if (latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) outcome.get() else Failure(timedOut)
  }

  /**
    * Whether a server-error reply to this command may be tolerated during bootstrap. `CLIENT SETINFO` qualifies because it is library
    * identification added in Redis 7.2, so an older server rejects it with an error every client ignores. Every other bootstrap command is
    * load-bearing, so its failure stays fatal.
    */
  def bestEffort(command: Command[?]): Boolean =
    command.name == "CLIENT" && command.args.headOption.exists(_.asUtf8String == "SETINFO")
}
