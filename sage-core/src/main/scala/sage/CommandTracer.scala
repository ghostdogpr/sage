package sage

import sage.cluster.Node
import sage.commands.Command

/**
  * Produces one tracing span per command. [[onCommand]] runs while the caller submits commands that are traced immediately. Commands whose
  * execution is decided later use [[prepare]]; implementations can override it to capture the active parent context before execution moves
  * to another thread. Unlike [[SageListener]], this interface is part of command execution and does not drop events. The core API is
  * independent of effect systems and tracing libraries; integration modules provide implementations. Tracers receive the command name but
  * not its arguments. Sage catches exceptions from tracers to keep command execution unaffected.
  */
trait CommandTracer {

  /**
    * Called once per command on the submitting fiber, where the parent context is available. The returned [[CommandSpan]] receives the routed
    * node when known and is completed with the command's outcome.
    */
  def onCommand(command: Command[?]): CommandSpan

  /**
    * Prepares tracing for a command whose execution may be decided later or on another thread, such as a cached read that reaches the server only
    * on a miss. Called on the submitting fiber to capture the parent context. The returned function starts the span if the command runs. The
    * default delegates lazily to [[onCommand]]; override it when the parent context must be captured before work leaves the submitting fiber.
    */
  def prepare(command: Command[?]): () => CommandSpan = () => onCommand(command)
}

/**
  * A span for one in-flight command, created by [[CommandTracer.onCommand]] and completed by the runtime.
  */
trait CommandSpan {

  /**
    * Records the node that handled the command. Called once before [[settled]], after cluster routing when applicable.
    */
  def routedTo(node: Node): Unit

  /**
    * Ends the span and records failures. The runtime calls this at most once, but implementations must tolerate repeated calls.
    */
  def settled(outcome: Outcome): Unit
}

object CommandSpan {

  /**
    * A span that records nothing, used when tracing is disabled.
    */
  val noop: CommandSpan = new CommandSpan {
    def routedTo(node: Node): Unit      = ()
    def settled(outcome: Outcome): Unit = ()
  }
}
