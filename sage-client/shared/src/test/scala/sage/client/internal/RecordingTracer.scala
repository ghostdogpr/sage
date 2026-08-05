package sage.client.internal

import scala.collection.mutable

import sage.{CommandSpan, CommandTracer, Outcome}
import sage.cluster.Node
import sage.commands.Command

// record each span's start, routed node, and outcome so tests can compare the lifecycle without OpenTelemetry
final class RecordingTracer extends CommandTracer {
  val log                                         = mutable.ArrayBuffer.empty[String]
  def onCommand(command: Command[?]): CommandSpan = {
    log += s"start:${command.name}"
    new CommandSpan {
      def routedTo(node: Node): Unit      = log += s"routed:${node.host}:${node.port}"
      def settled(outcome: Outcome): Unit = log += s"settled:$outcome"
    }
  }
}
