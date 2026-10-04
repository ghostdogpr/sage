package sage.opentelemetry

import scala.jdk.CollectionConverters.*

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData

import sage.{CommandTracer, Outcome}
import sage.cluster.Node
import sage.commands.Command

class OpenTelemetryCommandTracerSpec extends munit.FunSuite {

  // a fresh in-memory SDK + tracer per test, so finished spans never leak across tests
  private class Traced(peerService: String = "redis") {
    private val exporter      = InMemorySpanExporter.create()
    val sdk: OpenTelemetrySdk =
      OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()).build()
    val tracer: CommandTracer = OpenTelemetryCommandTracer(sdk, peerService)

    def onlySpan: SpanData =
      exporter.getFinishedSpanItems.asScala.toList match {
        case List(span) => span
        case spans      => fail(s"expected one finished span, got $spans")
      }
  }

  private def get(span: SpanData, key: String): String =
    span.getAttributes.get(AttributeKey.stringKey(key))

  private def command(name: String): Command[Unit] =
    Command(name, Command.NoKeys, Vector.empty, _ => Right(()))

  test("a successful command yields one CLIENT span named for the command, with the Datadog-parity attributes") {
    val t = Traced()

    t.tracer.onCommand(command("GET")).settled(Outcome.Succeeded)

    val span = t.onlySpan
    assertEquals(span.getName, "GET")
    assertEquals(span.getKind, SpanKind.CLIENT)
    assertEquals(get(span, "db.system"), "redis")
    assertEquals(get(span, "db.operation.name"), "GET")
    assertEquals(get(span, "peer.service"), "redis")
    assertEquals(get(span, "component"), "redis-client")
    assertEquals(span.getStatus.getStatusCode, StatusCode.UNSET)
  }

  test("peerService is configurable, collapsing a cluster's nodes into one dependency") {
    val t = Traced("my-redis")

    t.tracer.onCommand(command("SET")).settled(Outcome.Succeeded)

    assertEquals(get(t.onlySpan, "peer.service"), "my-redis")
  }

  test("routedTo sets the server address and port") {
    val t = Traced()

    val span = t.tracer.onCommand(command("GET"))
    span.routedTo(Node("redis.internal", 6380))
    span.settled(Outcome.Succeeded)

    val data = t.onlySpan
    assertEquals(get(data, "server.address"), "redis.internal")
    assertEquals(data.getAttributes.get(AttributeKey.longKey("server.port")), java.lang.Long.valueOf(6380L))
  }

  test("a failure records ERROR status and the exception") {
    val t = Traced()

    t.tracer.onCommand(command("GET")).settled(Outcome.Failed(new RuntimeException("boom")))

    val data = t.onlySpan
    assertEquals(data.getStatus.getStatusCode, StatusCode.ERROR)
    assert(data.getEvents.asScala.exists(_.getName == "exception"), "expected a recorded exception event")
  }

  test("the span nests under the active context's span") {
    val t      = Traced()
    val parent = t.sdk.getTracer("test").spanBuilder("request").startSpan()

    val scope = parent.makeCurrent()
    try t.tracer.onCommand(command("GET")).settled(Outcome.Succeeded)
    finally scope.close()

    assertEquals(t.onlySpan.getParentSpanContext, parent.getSpanContext)
  }

  test("prepare captures the parent context up front, so a span started after the context is gone still nests under it") {
    val t      = Traced()
    val parent = t.sdk.getTracer("test").spanBuilder("request").startSpan()

    // capture while the parent is current, then start the span after the scope is closed (mimicking a fetch on an offload worker)
    val scope     = parent.makeCurrent()
    val startSpan = t.tracer.prepare(command("GET"))
    scope.close()
    startSpan().settled(Outcome.Succeeded)

    assertEquals(t.onlySpan.getParentSpanContext, parent.getSpanContext)
  }

  test("settling twice ends the span only once") {
    val t = Traced()

    val span = t.tracer.onCommand(command("GET"))
    span.settled(Outcome.Succeeded)
    span.settled(Outcome.Failed(new RuntimeException("late")))

    assertEquals(t.onlySpan.getStatus.getStatusCode, StatusCode.UNSET)
  }
}
