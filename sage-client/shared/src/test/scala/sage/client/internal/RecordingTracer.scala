package sage.client.internal

import scala.collection.mutable

import sage.{CommandSpan, CommandTracer, Outcome}
import sage.cluster.Node
import sage.commands.Command

// record each span's start, routed node, and outcome so tests can compare the lifecycle without OpenTelemetry
final class RecordingTracer extends CommandTracer {
  val log                                         = mutable.ArrayBuffer.empty[String]
  // spans of one command can start and settle on different threads
  private def record(entry: String): Unit         = log.synchronized(log += entry): Unit
  def onCommand(command: Command[?]): CommandSpan = {
    record(s"start:${command.name}")
    new CommandSpan {
      def routedTo(node: Node): Unit      = record(s"routed:${node.host}:${node.port}")
      def settled(outcome: Outcome): Unit = record(s"settled:$outcome")
    }
  }
}
