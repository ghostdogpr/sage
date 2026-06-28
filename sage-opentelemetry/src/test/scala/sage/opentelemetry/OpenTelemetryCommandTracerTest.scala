package sage.opentelemetry

import scala.jdk.CollectionConverters.*

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.SdkTracerProvider

import sage.Outcome
import sage.cluster.Node
import sage.commands.Command

class OpenTelemetryCommandTracerTest extends munit.FunSuite {

  // a fresh in-memory SDK + tracer per test, so finished spans never leak across tests
  private def fixture: (OpenTelemetrySdk, InMemorySpanExporter) = {
    val exporter = InMemorySpanExporter.create()
    val sdk      = OpenTelemetrySdk
      .builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
      .build()
    (sdk, exporter)
  }

  private def get(span: io.opentelemetry.sdk.trace.data.SpanData, key: String): String =
    span.getAttributes.get(AttributeKey.stringKey(key))

  private def command(name: String): Command[Unit] =
    Command(name, Command.NoKeys, Vector.empty, _ => Right(()))

  test("a successful command yields one CLIENT span named for the command, with the Datadog-parity attributes") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)

    tracer.onCommand(command("GET")).settled(Outcome.Succeeded)

    val spans = exporter.getFinishedSpanItems.asScala.toList
    assertEquals(spans.size, 1)
    val span  = spans.head
    assertEquals(span.getName, "GET")
    assertEquals(span.getKind, SpanKind.CLIENT)
    assertEquals(get(span, "db.system"), "redis")
    assertEquals(get(span, "db.operation.name"), "GET")
    assertEquals(get(span, "peer.service"), "redis")
    assertEquals(get(span, "component"), "redis-client")
    assertEquals(span.getStatus.getStatusCode, StatusCode.UNSET)
  }

  test("peerService is configurable, collapsing a cluster's nodes into one dependency") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk, peerService = "my-redis")

    tracer.onCommand(command("SET")).settled(Outcome.Succeeded)

    assertEquals(get(exporter.getFinishedSpanItems.asScala.head, "peer.service"), "my-redis")
  }

  test("routedTo sets the server address and port") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)

    val span = tracer.onCommand(command("GET"))
    span.routedTo(Node("redis.internal", 6380))
    span.settled(Outcome.Succeeded)

    val data = exporter.getFinishedSpanItems.asScala.head
    assertEquals(get(data, "server.address"), "redis.internal")
    assertEquals(data.getAttributes.get(AttributeKey.longKey("server.port")), java.lang.Long.valueOf(6380L))
  }

  test("a failure records ERROR status and the exception") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)

    tracer.onCommand(command("GET")).settled(Outcome.Failed(new RuntimeException("boom")))

    val data = exporter.getFinishedSpanItems.asScala.head
    assertEquals(data.getStatus.getStatusCode, StatusCode.ERROR)
    assert(data.getEvents.asScala.exists(_.getName == "exception"), "expected a recorded exception event")
  }

  test("the span nests under the active context's span") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)
    val parent          = sdk.getTracer("test").spanBuilder("request").startSpan()

    val scope = parent.makeCurrent()
    try tracer.onCommand(command("GET")).settled(Outcome.Succeeded)
    finally scope.close()
    parent.end()

    val spans  = exporter.getFinishedSpanItems.asScala.toList
    val redis  = spans.find(_.getName == "GET").get
    val server = spans.find(_.getName == "request").get
    assertEquals(redis.getParentSpanContext.getSpanId, server.getSpanContext.getSpanId)
    assertEquals(redis.getTraceId, server.getTraceId)
  }

  test("prepare captures the parent context up front, so a span started after the context is gone still nests under it") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)
    val parent          = sdk.getTracer("test").spanBuilder("request").startSpan()

    // capture while the parent is current, then start the span after the scope is closed (mimicking a fetch on an offload worker)
    val scope     = parent.makeCurrent()
    val startSpan = tracer.prepare(command("GET"))
    scope.close()
    startSpan().settled(Outcome.Succeeded)
    parent.end()

    val spans = exporter.getFinishedSpanItems.asScala.toList
    val redis = spans.find(_.getName == "GET").get
    assertEquals(redis.getParentSpanContext.getSpanId, parent.getSpanContext.getSpanId)
    assertEquals(redis.getTraceId, parent.getSpanContext.getTraceId)
  }

  test("settling twice ends the span only once") {
    val (sdk, exporter) = fixture
    val tracer          = OpenTelemetryCommandTracer(sdk)

    val span = tracer.onCommand(command("GET"))
    span.settled(Outcome.Succeeded)
    span.settled(Outcome.Failed(new RuntimeException("late")))

    val spans = exporter.getFinishedSpanItems.asScala.toList
    assertEquals(spans.size, 1)
    assertEquals(spans.head.getStatus.getStatusCode, StatusCode.UNSET)
  }
}
