package sage.client.internal

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import sage.CommandSpan
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.Command

/**
  * One cluster Node's connection bundle: the same Multiplexed Connection + Dedicated Pool a standalone client uses, pinned to a single
  * node. The cluster runtime holds one per master it routes to. `asking` prefixes the command with `ASKING` to follow an `ASK` redirect.
  */
final private[client] class NodeClient(connection: MultiplexedConnection, pool: DedicatedPool) {

  // `lease` (blocking only) lets an interrupted caller release the leased slot; null falls back to a private, uncancellable lease
  def submit[A](command: Command[A], asking: Boolean, callback: Try[A] => Unit, lease: DedicatedPool.Lease = null): Unit =
    if (command.isBlocking) {
      val l = if (lease != null) lease else new DedicatedPool.Lease
      if (asking) pool.useAsking(command, callback, l) else pool.use(command, callback, l)
    } else if (asking) connection.submitAsking(command, callback)
    else connection.submit(command, callback)

  def cachedSubmit[A](command: Command[A], ttlMillis: Long, callback: Try[A] => Unit, deferred: () => CommandSpan): Unit =
    connection.cachedSubmit(command, ttlMillis, callback, deferred)

  // a pipeline's per-node batch: one round-trip on this node's Multiplexed Connection. False when not connected (nothing submitted), so
  // the caller re-routes each position rather than fabricating per-position errors. Blocking commands never reach here (rejected up front).
  def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean =
    connection.submitAll(commands, callbacks)

  def acquireForTransaction(): DedicatedConnection = pool.acquireForTransaction()

  def releaseTransaction(connection: DedicatedConnection, reusable: Boolean): Unit = pool.releaseTransaction(connection, reusable)

  def isLive: Boolean = connection.isLive

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
    dedicatedPool: DedicatedPoolConfig,
    cacheMaxBytes: Long = 0L,
    node: Option[Node] = None,
    events: Events = Events.disabled,
    dedicatedBootstrap: Option[Vector[Command[?]]] = None
  ): NodeClient = {
    val connection =
      MultiplexedConnection.connect(factory, scheduler, bootstrap, reconnect, watchdog, connectTimeout, closeTimeout, cacheMaxBytes, node, events)
    val pool       =
      DedicatedPool.forConnection(factory, dedicatedBootstrap.getOrElse(bootstrap), scheduler, connection, dedicatedPool, connectTimeout.toMillis)
    new NodeClient(connection, pool)
  }
}
