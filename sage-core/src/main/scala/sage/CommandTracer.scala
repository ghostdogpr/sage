package sage

import sage.cluster.Node
import sage.commands.Command

/**
  * Produces one tracing span per command, synchronously on the command path. Unlike a [[SageListener]] (asynchronous and lossy, with no access
  * to the caller's context), a tracer runs on the caller's fiber as the command is submitted, so its spans nest under the active request. It is
  * effect-agnostic and dependency-free, so it lives in the core and an integration module binds to it. A tracer sees the command's name but
  * never its arguments, keeping secrets out of traces. sage guards every call, so a throwing tracer cannot break command execution.
  */
trait CommandTracer {

  /**
    * Called once per command, on the caller's fiber as it is submitted — where the parent context is live. The returned [[CommandSpan]] is
    * given its routed node (when known) and settled when the command does.
    */
  def onCommand(command: Command[?]): CommandSpan

  /**
    * For a command whose execution — and so whether a span is even needed — is decided later, possibly on another thread: a cached read that
    * only reaches the server on a miss. Called on the caller's fiber to capture the live parent context; the returned thunk starts the span with
    * that parent if and when the command actually runs (never, for a local cache hit). The default starts the span lazily via [[onCommand]];
    * override when the parent context must be captured up front, before the deferred work moves off the caller's fiber.
    */
  def prepare(command: Command[?]): () => CommandSpan = () => onCommand(command)
}

/**
  * A single in-flight command span, handed out by [[CommandTracer.onCommand]] and driven to completion by the runtime.
  */
trait CommandSpan {

  /**
    * The node this command reached; sets the span's server-address attributes. Called once before [[settled]] (after routing, for a cluster).
    */
  def routedTo(node: Node): Unit

  /**
    * Ends the span, recording an error status on a failure. The runtime settles at most once, but an implementation must tolerate a repeat.
    */
  def settled(outcome: Outcome): Unit
}

object CommandSpan {

  /**
    * A span that records nothing, returned when no tracer is set so the runtime need not branch on whether a span exists.
    */
  val noop: CommandSpan = new CommandSpan {
    def routedTo(node: Node): Unit      = ()
    def settled(outcome: Outcome): Unit = ()
  }
}
