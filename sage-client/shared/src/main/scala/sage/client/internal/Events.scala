package sage.client.internal

import java.util.concurrent.ArrayBlockingQueue

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.util.Try
import scala.util.control.NonFatal

import sage.{CommandSpan, CommandTracer, Outcome, SageEvent, SageListener}
import sage.cluster.Node
import sage.commands.Command

/**
  * Carries two independent observability integrations. [[emit]] feeds the asynchronous [[SageListener]] path: called from the runtime's hot
  * paths (the reply thread, routing offloads), it must never block, so it does a single non-blocking enqueue drained by a daemon thread.
  * [[tracer]] feeds the synchronous [[CommandTracer]] path, whose spans start and settle inline on the command path. With neither registered
  * the whole thing is a no-op ([[Events.disabled]]) and no thread runs.
  */
private[client] trait Events {
  def enabled: Boolean
  def emitsEvents: Boolean
  def tracer: Option[CommandTracer]
  // the server a span is routed to when routing resolves no node itself (standalone's endpoint); None for cluster/master-replica
  def serverNode: Option[Node]
  def emit(event: SageEvent): Unit
  def close(): Unit
}

private[client] object Events {

  // a healthy listener never fills this; one that does is misbehaving, and dropping (drop-newest) is the only relief that never blocks the
  // producer. Deliberately not a config knob — promotable backward-compatibly if a real need appears.
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
    * Drives the two integrations. Listeners, when present, are fanned to from one bounded queue drained by a single daemon thread, each call
    * guarded so a throwing listener cannot kill the loop or affect its peers; a full queue drops the newest event silently. A tracer carries no
    * thread of its own — it runs inline on the command path. With a tracer but no listener, no queue and no thread exist.
    */
  final private class Live(listeners: Vector[SageListener], val tracer: Option[CommandTracer], val serverNode: Option[Node]) extends Events {

    def enabled: Boolean     = true
    def emitsEvents: Boolean = listeners.nonEmpty

    private val queue             = if (listeners.isEmpty) null else new ArrayBlockingQueue[SageEvent](QueueDepth)
    @volatile private var running = true

    private val worker =
      if (listeners.isEmpty) null
      else {
        val t = new Thread(() => drain(), "sage-listener")
        t.setDaemon(true)
        t.start()
        t
      }

    def emit(event: SageEvent): Unit = if (queue != null) { val _ = queue.offer(event) }

    def close(): Unit = if (worker != null) { running = false; worker.interrupt() }

    private def drain(): Unit = {
      try while (running) dispatch(queue.take())
      catch { case _: InterruptedException => () }
      // best-effort: deliver what is already queued before exiting
      var event = queue.poll()
      while (event != null) { dispatch(event); event = queue.poll() }
    }

    private def dispatch(event: SageEvent): Unit = {
      var i = 0
      while (i < listeners.length) {
        try listeners(i).onEvent(event)
        catch { case NonFatal(_) => () }
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

  // like startSpan but lazy: capture the caller's context now, start the span only when the thunk is invoked (a cache miss), via startDeferred
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

  // set on the tracking callback by the routing layer at the node-known terminal site, just before it completes; a no-op for any other callback
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

    def at(n: Node): Unit = { node = Some(n); routeSpan(span, n) }

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
