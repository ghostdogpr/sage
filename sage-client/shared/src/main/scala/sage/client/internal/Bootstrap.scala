package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.{Failure, Success, Try}

import sage.SageException.{ConnectionLost, ServerError}
import sage.client.{AuthConfig, BuildInfo}
import sage.commands.{Command, Connection}

private[client] object Bootstrap {

  /**
    * The setup commands every connection runs after `HELLO` and re-runs on reconnection, shared by the standalone, cluster, and
    * master-replica paths so identification is never applied to one topology and dropped on another. `SELECT` lives here, never as a
    * runtime command, because it would move the database under every fiber sharing the connection.
    */
  def commands(auth: Option[AuthConfig], database: Int, clientName: Option[String]): Vector[Command[?]] = {
    val identification = Vector(
      Connection.clientSetInfo("LIB-NAME", "sage"),
      Connection.clientSetInfo("LIB-VER", BuildInfo.version)
    ) ++ clientName.map(Connection.clientSetName).toVector
    val selectDb       = if (database > 0) Vector(Connection.select(database)) else Vector.empty
    (Connection.hello(auth.map(a => a.username -> a.password)) +: identification) ++ selectDb
  }

  /**
    * Runs the connection-setup handshake on a freshly opened connection: submits each command in turn and blocks for its reply up to
    * `connectTimeoutMillis`, then closes the half-built connection and throws on a timeout or a failed reply so the caller discards it.
    * `submit` enqueues one command and delivers its decoded reply; replies are FIFO, so each command is awaited before the next is sent.
    * A [[bestEffort]] command whose reply is a `ServerError` is tolerated rather than fatal (and reported to `onTolerated`); a
    * `ConnectionLost`/`DecodeError` stays fatal even for it. Used by the Multiplexed, Dedicated, and Subscription connections, which differ
    * only in how a reply is obtained.
    */
  def run(
    commands: Vector[Command[?]],
    connectTimeoutMillis: Long,
    submit: (Command[?], Try[Any] => Unit) => Unit,
    close: () => Unit,
    onTolerated: Command[?] => Unit = _ => ()
  ): Unit =
    commands.foreach { command =>
      val latch   = new CountDownLatch(1)
      val outcome = new AtomicReference[Try[Any]]()
      submit(command, result => { outcome.set(result); latch.countDown() })
      if (!latch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)) {
        close()
        throw ConnectionLost(mayHaveExecuted = false)
      }
      outcome.get() match {
        case Failure(_: ServerError) if bestEffort(command) => onTolerated(command)
        case Failure(error)                                 => close(); throw error
        case Success(_)                                     => ()
      }
    }

  /**
    * Whether a server-error reply to this command may be tolerated during bootstrap. `CLIENT SETINFO` qualifies because it is library
    * identification added in Redis 7.2, so an older server rejects it with an error every client ignores. `CLIENT TRACKING` qualifies because
    * a server that permits `HELLO` but denies tracking (an ACL restriction, a proxy) should still connect and serve cached reads uncached
    * rather than fail the connection (ADR-0045). Every other bootstrap command is load-bearing, so its failure stays fatal.
    */
  private def bestEffort(command: Command[?]): Boolean =
    command.name == "CLIENT" && command.args.headOption.map(_.asUtf8String).exists(sub => sub == "SETINFO" || sub == "TRACKING")
}
