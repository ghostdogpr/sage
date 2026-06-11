package sage.client.internal

import java.util.concurrent.ArrayBlockingQueue

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.util.Try
import scala.util.control.NonFatal

import sage.{Outcome, SageEvent, SageListener}
import sage.cluster.Node
import sage.commands.Command

/**
  * The event-delivery seam. [[emit]] is called from the runtime's hot paths (the reply thread, routing offloads) and must never block: it
  * does a single non-blocking enqueue. A slow or throwing listener can therefore neither stall command execution nor corrupt it. When no
  * listener is registered the whole thing is a no-op ([[Events.disabled]]) and no thread runs.
  */
private[client] trait Events {
  def enabled: Boolean
  def emit(event: SageEvent): Unit
  def close(): Unit
}

private[client] object Events {

  // a healthy listener never fills this; one that does is misbehaving, and dropping (drop-newest) is the only relief that never blocks the
  // producer. Deliberately not a config knob — promotable backward-compatibly if a real need appears.
  final private val QueueDepth = 1024

  val disabled: Events = new Events {
    def enabled: Boolean             = false
    def emit(event: SageEvent): Unit = ()
    def close(): Unit                = ()
  }

  def apply(listeners: Vector[SageListener]): Events =
    if (listeners.isEmpty) disabled else new Dispatcher(listeners)

  /**
    * One bounded queue drained by a single daemon thread that fans each event to every listener, each call guarded so a throwing listener
    * cannot kill the loop or affect its peers. A full queue drops the newest event silently.
    */
  final private class Dispatcher(listeners: Vector[SageListener]) extends Events {

    private val queue             = new ArrayBlockingQueue[SageEvent](QueueDepth)
    @volatile private var running = true

    private val worker = {
      val t = new Thread(() => drain(), "sage-listener")
      t.setDaemon(true)
      t.start()
      t
    }

    def enabled: Boolean             = true
    def emit(event: SageEvent): Unit = { val _ = queue.offer(event) }
    def close(): Unit                = { running = false; worker.interrupt() }

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

  /**
    * Wraps a command's terminal reply callback so it emits a [[SageEvent.CommandCompleted]] when the command settles, timing from the wrap.
    * The node is left unset until the routing layer attributes it via [[attributeNode]] (standalone never does, so it stays `None`). Returns
    * the callback unchanged when events are disabled, so a client without listeners pays nothing.
    */
  def trackCommand[A](events: Events, command: Command[?], callback: Try[A] => Unit): Try[A] => Unit =
    if (!events.enabled) callback else new CommandEmit[A](command.name, System.nanoTime(), events, callback)

  // set on the tracking callback by the routing layer at the node-known terminal site, just before it completes; a no-op for any other callback
  def attributeNode(callback: AnyRef, node: Node): Unit =
    callback match {
      case emit: CommandEmit[?] => emit.at(node)
      case _                    => ()
    }

  final private class CommandEmit[A](command: String, startNanos: Long, events: Events, callback: Try[A] => Unit) extends (Try[A] => Unit) {

    @volatile private var node: Option[Node] = None

    def at(n: Node): Unit = node = Some(n)

    def apply(result: Try[A]): Unit = {
      events.emit(SageEvent.CommandCompleted(command, node, FiniteDuration(System.nanoTime() - startNanos, NANOSECONDS), Outcome.of(result)))
      callback(result)
    }
  }
}
