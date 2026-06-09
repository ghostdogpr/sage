package sage.client.internal

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.commands.Command

/**
  * One cluster Node's connection bundle: the same Multiplexed Connection + Dedicated Pool a standalone client uses, pinned to a single
  * node. The cluster runtime holds one per master it routes to. `asking` prefixes the command with `ASKING` to follow an `ASK` redirect.
  */
final private[client] class NodeClient(connection: MultiplexedConnection, pool: DedicatedPool) {

  def submit[A](command: Command[A], asking: Boolean, callback: Try[A] => Unit): Unit =
    if (command.isBlocking) { if (asking) pool.useAsking(command, callback) else pool.use(command, callback) }
    else if (asking) connection.submitAsking(command, callback)
    else connection.submit(command, callback)

  def isLive: Boolean = connection.currentState == MultiplexedConnection.State.Live

  def close(): Unit = {
    pool.close()
    connection.close()
  }
}

private[client] object NodeClient {

  /**
    * Connects to the node and runs the bootstrap synchronously, throwing (no retry) if the first handshake fails — exactly like a
    * standalone client. The caller offloads this blocking establish and treats a failure as the node being unreachable.
    */
  def connect(
    factory: MultiplexedConnection.TransportFactory,
    scheduler: Scheduler,
    bootstrap: Vector[Command[?]],
    reconnect: BackoffConfig,
    watchdog: WatchdogConfig,
    connectTimeout: FiniteDuration,
    closeTimeout: FiniteDuration,
    dedicatedPool: DedicatedPoolConfig
  ): NodeClient = {
    val connection = MultiplexedConnection.connect(factory, scheduler, bootstrap, reconnect, watchdog, connectTimeout, closeTimeout)
    val pool       = new DedicatedPool(
      factory,
      bootstrap,
      scheduler,
      () => connection.currentState == MultiplexedConnection.State.Live,
      () => connection.currentGeneration,
      dedicatedPool,
      connectTimeout.toMillis
    )
    new NodeClient(connection, pool)
  }
}
