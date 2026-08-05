package sage.client.internal

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.util.Try
import scala.util.control.NonFatal

import sage.{CommandSpan, CommandTracer, Outcome, SageEvent, SageListener}
import sage.cluster.Node
import sage.commands.Command

/**
  * Provides event listeners and command tracing. [[emit]] adds listener events to a bounded non-blocking queue. A daemon thread takes events
  * from that queue and calls the listeners. [[tracer]] runs synchronously with command execution. [[Events.disabled]] avoids allocating the queue and thread when neither
  * integration is configured.
  */
private[client] trait Events {
  def enabled: Boolean
  def emitsEvents: Boolean
  def tracer: Option[CommandTracer]
  // The standalone server associated with a tracing span. Cluster and master-replica routing assign their node later, so they use None here.
  def serverNode: Option[Node]
  def emit(event: SageEvent): Unit
  def close(): Unit
}

private[client] object Events {

  // drop the newest event if a listener cannot keep up, keeping command processing non-blocking
  final private val QueueDepth = 1024

  val disabled: Events = new Events {
    def enabled: Boolean              = false
    def emitsEvents: Boolean          = false
    def tracer: Option[CommandTracer] = None
    def serverNode: Option[Node]      = None
    def emit(event: SageEvent): Unit  = ()
    def close(): Unit                 = ()
  }

  def apply(listeners: Vector[SageListener], tracer: Option[CommandTracer] = None, serverNode: Option[Node] = None): Events =
    if (listeners.isEmpty && tracer.isEmpty) disabled else new Live(listeners, tracer, serverNode)

  /**
    * Runs the configured listener and tracer integrations. One daemon thread sends queued events to all listeners. Listener exceptions are
    * ignored so they do not stop delivery to other listeners. A full queue drops the newest event. Tracing runs inline with commands and
    * does not require the listener queue or thread.
    */
  final private class Live(listeners: Vector[SageListener], val tracer: Option[CommandTracer], val serverNode: Option[Node]) extends Events {

    def enabled: Boolean     = true
    def emitsEvents: Boolean = listeners.nonEmpty

    private val queue             = if (listeners.isEmpty) null else new ArrayBlockingQueue[SageEvent](QueueDepth)
    private val failedConnections = ConcurrentHashMap.newKeySet[Node]()
    @volatile private var running = true

    private val worker =
      if (listeners.isEmpty) null
      else {
        val t = new Thread(() => drain(), "sage-listener")
        t.setDaemon(true)
        t.start()
        t
      }

    def emit(event: SageEvent): Unit =
      if (queue != null)
        event match {
          case failure @ SageEvent.Connection.ConnectFailed(Some(node), _) =>
            if (failedConnections.add(node) && !queue.offer(failure)) { failedConnections.remove(node): Unit }
          case connected @ SageEvent.Connection.Connected(Some(node))      =>
            failedConnections.remove(node)
            queue.offer(connected): Unit
          case _                                                           =>
            queue.offer(event): Unit
        }

    def close(): Unit = if (worker != null) {
      running = false
      worker.interrupt()
    }

    private def drain(): Unit = {
      // Continue after an interrupt while running because a listener may leave the interrupt flag set. close clears running before it
      // interrupts the worker, which lets shutdown end the loop.
      while (running)
        try dispatch(queue.take())
        catch { case _: InterruptedException => () }
      // best-effort: deliver what is already queued before exiting
      var event = queue.poll()
      while (event != null) {
        dispatch(event)
        event = queue.poll()
      }
    }

    private def dispatch(event: SageEvent): Unit = {
      var i = 0
      while (i < listeners.length) {
        // catch InterruptedException as well as other listener failures. One listener must not prevent delivery to the remaining listeners.
        try listeners(i).onEvent(event)
        catch {
          case _: InterruptedException => ()
          case NonFatal(_)             => ()
        }
        i += 1
      }
    }
  }

  // start the span on the caller's fiber, where the parent context is live; the duration clock starts later, in trackCommand
  def startSpan(events: Events, command: Command[?]): CommandSpan =
    events.tracer match {
      case Some(t) =>
        val span =
          try t.onCommand(command)
          catch { case NonFatal(_) => CommandSpan.noop }
        routeToServerNode(events, span)
        span
      case None    => CommandSpan.noop
    }

  private val noSpanFactory: () => CommandSpan = () => CommandSpan.noop

  // Capture tracing context now and return a function that starts the span later. Cached reads call it only when a cache miss reaches the server.
  def deferSpan(events: Events, command: Command[?]): () => CommandSpan =
    events.tracer match {
      case Some(t) =>
        try t.prepare(command)
        catch { case NonFatal(_) => noSpanFactory }
      case None    => noSpanFactory
    }

  def startDeferred(factory: () => CommandSpan): CommandSpan =
    try factory()
    catch { case NonFatal(_) => CommandSpan.noop }

  def startOrDefer(events: Events, command: Command[?], deferred: () => CommandSpan): CommandSpan =
    if (deferred == null) startSpan(events, command) else startDeferred(deferred)

  def startSpans(events: Events, commands: Vector[Command[?]]): Vector[CommandSpan] =
    if (events.tracer.isEmpty) Vector.empty else commands.map(c => startSpan(events, c))

  def deferSpans(events: Events, commands: Vector[Command[?]]): Vector[() => CommandSpan] =
    if (events.tracer.isEmpty) Vector.empty else commands.map(c => deferSpan(events, c))

  def trackCommand[A](events: Events, command: Command[?], callback: Try[A] => Unit): Try[A] => Unit =
    if (!events.enabled) callback else new CommandEmit[A](command.name, System.nanoTime(), events, callback, startSpan(events, command))

  // overload taking a span already started on the caller's fiber, for offloaded paths; only the duration clock starts here
  def trackCommand[A](events: Events, command: Command[?], callback: Try[A] => Unit, span: CommandSpan): Try[A] => Unit =
    if (!events.enabled) callback else new CommandEmit[A](command.name, System.nanoTime(), events, callback, span)

  // traces a transaction's commands without emitting a CommandCompleted, so they stay invisible to listeners
  def trackSpan[A](events: Events, command: Command[?], callback: Try[A] => Unit): Try[A] => Unit =
    if (events.tracer.isEmpty) callback
    else new CommandEmit[A](command.name, System.nanoTime(), events, callback, startSpan(events, command), emitsEvent = false)

  // Record the final routed node before the command completes. Ignore callbacks that do not track command events.
  def attributeNode(callback: AnyRef, node: Node): Unit =
    callback match {
      case emit: CommandEmit[?] => emit.at(node)
      case _                    => ()
    }

  def abandonSpan(callback: AnyRef, error: Throwable): Unit =
    callback match {
      case emit: CommandEmit[?] => emit.abandon(error)
      case _                    => ()
    }

  private def routeToServerNode(events: Events, span: CommandSpan): Unit =
    events.serverNode match {
      case Some(node) => routeSpan(span, node)
      case None       => ()
    }

  def routeSpan(span: CommandSpan, node: Node): Unit =
    try span.routedTo(node)
    catch { case NonFatal(_) => () }

  def settleSpan(span: CommandSpan, outcome: Outcome): Unit =
    try span.settled(outcome)
    catch { case NonFatal(_) => () }

  final private class CommandEmit[A](
    name: String,
    startNanos: Long,
    events: Events,
    callback: Try[A] => Unit,
    span: CommandSpan,
    emitsEvent: Boolean = true
  ) extends (Try[A] => Unit) {

    @volatile private var node: Option[Node] = None

    def at(n: Node): Unit = {
      node = Some(n)
      routeSpan(span, n)
    }

    def abandon(error: Throwable): Unit = settleSpan(span, Outcome.Failed(error))

    def apply(result: Try[A]): Unit = {
      val outcome = Outcome.of(result)
      settleSpan(span, outcome)
      if (emitsEvent && events.emitsEvents)
        events.emit(SageEvent.CommandCompleted(name, node, FiniteDuration(System.nanoTime() - startNanos, NANOSECONDS), outcome))
      callback(result)
    }
  }
}
