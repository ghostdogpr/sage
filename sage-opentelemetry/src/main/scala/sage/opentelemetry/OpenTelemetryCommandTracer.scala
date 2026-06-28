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
  * A [[CommandTracer]] emitting one OpenTelemetry `CLIENT` span per command, attributed to match Datadog's Lettuce instrumentation: the span
  * name is the command (e.g. `GET`), `db.system` is `redis`, and `peer.service` collapses a cluster's nodes into one Redis dependency. The
  * span's parent is the context `contextProvider` reads when the command is submitted — by default the thread-local current context, correct
  * wherever an APM agent propagates context across thread pools; a bare OpenTelemetry SDK on a fiber runtime needs a fiber-native provider
  * instead. Only the command name is recorded; arguments and keys never reach a span.
  */
final class OpenTelemetryCommandTracer private (
  tracer: Tracer,
  peerService: String,
  contextProvider: () => Context
) extends CommandTracer {

  def onCommand(command: Command[?]): CommandSpan = startSpan(command, contextProvider())

  // capture the caller's context now; start the span only when the deferred work runs (a cache miss's fetch), so a local hit starts none
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
    * Builds a tracer from an explicit [[OpenTelemetry]] (the testable form: inject an in-memory SDK), reading the thread-local current context
    * as each command's parent. `peerService` is the name every Redis node reports as, so a cluster shows as a single dependency; default `redis`.
    */
  def apply(openTelemetry: OpenTelemetry, peerService: String = "redis"): CommandTracer =
    new OpenTelemetryCommandTracer(openTelemetry.getTracer("sage"), peerService, () => Context.current())

  /**
    * Builds a tracer from the globally-registered [[OpenTelemetry]] — the zero-configuration form for an APM agent (e.g. the Datadog Java agent
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

    // the runtime settles a span at most once, but a fail-fast path and a late callback could race; guard so the span ends exactly once
    private val ended = new AtomicBoolean(false)

    def routedTo(node: Node): Unit = {
      val _ = span.setAttribute(ServerAddress, node.host)
      val _ = span.setAttribute(ServerPort, node.port.toLong)
    }

    def settled(outcome: Outcome): Unit =
      if (ended.compareAndSet(false, true)) {
        outcome match {
          case Outcome.Succeeded   => ()
          case Outcome.Failed(err) =>
            val _ = span.setStatus(StatusCode.ERROR, Option(err.getMessage).getOrElse(err.getClass.getName))
            val _ = span.recordException(err)
        }
        span.end()
      }
  }
}
