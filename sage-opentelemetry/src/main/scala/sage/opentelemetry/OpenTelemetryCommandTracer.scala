package sage.opentelemetry

import java.util.concurrent.atomic.AtomicBoolean

import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanKind, StatusCode, Tracer}
import io.opentelemetry.context.Context

import sage.{CommandSpan, CommandTracer, Outcome}
import sage.cluster.Node
import sage.commands.Command

/**
  * A [[CommandTracer]] that creates one OpenTelemetry `CLIENT` span per command. The attributes follow Datadog's Lettuce instrumentation:
  * the span name is the command, `db.system` is `redis`, and `peer.service` groups all cluster nodes under one dependency. By default, the
  * span uses the current thread-local context as its parent. This works when an APM agent propagates context across thread pools. A bare
  * OpenTelemetry SDK on a fiber runtime should provide its fiber-local context instead. Spans contain the command name but not arguments
  * or keys.
  */
final class OpenTelemetryCommandTracer private (
  tracer: Tracer,
  peerService: String,
  contextProvider: () => Context
) extends CommandTracer {

  def onCommand(command: Command[?]): CommandSpan = startSpan(command, contextProvider())

  // capture the caller's context now, but create the span only if deferred work reaches the server. A local cache hit creates no span.
  override def prepare(command: Command[?]): () => CommandSpan = {
    val parent = contextProvider()
    () => startSpan(command, parent)
  }

  private def startSpan(command: Command[?], parent: Context): CommandSpan = {
    val span = tracer
      .spanBuilder(command.name)
      .setSpanKind(SpanKind.CLIENT)
      .setParent(parent)
      .setAttribute(OpenTelemetryCommandTracer.DbSystem, "redis")
      .setAttribute(OpenTelemetryCommandTracer.DbOperation, command.name)
      .setAttribute(OpenTelemetryCommandTracer.PeerService, peerService)
      .setAttribute(OpenTelemetryCommandTracer.Component, "redis-client")
      .startSpan()
    new OpenTelemetryCommandTracer.Span(span)
  }
}

object OpenTelemetryCommandTracer {

  private val DbSystem      = AttributeKey.stringKey("db.system")
  private val DbOperation   = AttributeKey.stringKey("db.operation.name")
  private val PeerService   = AttributeKey.stringKey("peer.service")
  private val Component     = AttributeKey.stringKey("component")
  private val ServerAddress = AttributeKey.stringKey("server.address")
  private val ServerPort    = AttributeKey.longKey("server.port")

  /**
    * Builds a tracer from an `OpenTelemetry` instance and uses the current thread-local context as each command's parent. `peerService`
    * groups all Redis nodes under one dependency and defaults to `redis`. Passing an explicit instance also supports tests with an
    * in-memory SDK.
    */
  def apply(openTelemetry: OpenTelemetry, peerService: String = "redis"): CommandTracer =
    new OpenTelemetryCommandTracer(openTelemetry.getTracer("sage"), peerService, () => Context.current())

  /**
    * Builds a tracer from the globally-registered `OpenTelemetry` — the zero-configuration form for an APM agent (e.g. the Datadog Java agent
    * with `dd.trace.otel.enabled=true`) that installs itself as the global instance.
    */
  def global(peerService: String = "redis"): CommandTracer =
    apply(GlobalOpenTelemetry.get(), peerService)

  /**
    * The advanced form: supply a custom parent-context provider. Use a fiber-native provider (reading a ZIO `FiberRef` or a cats-effect
    * `IOLocal`) when running a bare OpenTelemetry SDK with no APM agent on a fiber runtime, where the thread-local current context is empty.
    */
  def withContextProvider(openTelemetry: OpenTelemetry, peerService: String, contextProvider: () => Context): CommandTracer =
    new OpenTelemetryCommandTracer(openTelemetry.getTracer("sage"), peerService, contextProvider)

  final private class Span(span: io.opentelemetry.api.trace.Span) extends CommandSpan {

    // a fast failure can race with a late callback. End the span only for the first outcome.
    private val ended = new AtomicBoolean(false)

    def routedTo(node: Node): Unit = {
      span.setAttribute(ServerAddress, node.host)
      span.setAttribute(ServerPort, node.port.toLong): Unit
    }

    def settled(outcome: Outcome): Unit =
      if (ended.compareAndSet(false, true)) {
        outcome match {
          case Outcome.Succeeded   => ()
          case Outcome.Failed(err) =>
            span.setStatus(StatusCode.ERROR, Option(err.getMessage).getOrElse(err.getClass.getName))
            span.recordException(err)
        }
        span.end()
      }
  }
}
