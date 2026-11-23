package sage.integration

import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.util.Using

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainersSuite
import kyo.compat.*
import munit.{Location, TestOptions}
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder
import org.rnorth.ducttape.unreliables.Unreliables
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy

import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.{Client, Subscription}

/**
  * Shared connect-and-teardown helpers for the testcontainers suites: build a config for a started container, and run a body against a
  * client that is always closed afterwards.
  */
trait ContainerClient extends ClientAssertions with TestContainersSuite {

  // The Ox cell's unsafeRun uses this value. Keeping it non-private avoids unused-private warnings in the other cells.
  given ExecutionContext = munitExecutionContext

  protected def serverDef(image: String, exposedPorts: Seq[Int] = Seq(6379), command: Seq[String] = Seq()): GenericContainer.Def[GenericContainer] =
    GenericContainer.Def(image, exposedPorts = exposedPorts, command = command, waitStrategy = new AnswersPing)

  // container hooks run outside munitTimeout
  protected def prepare(setup: CIO[Unit]): Unit = Await.result(setup.unsafeRun, 3.minutes)

  protected def containerTest(options: TestOptions)(body: Containers => CIO[Any])(using Location): Unit =
    test(options)(withContainers(body(_).unsafeRun))

  // the suites build every config from this, so SAGE_IT_PROTOCOL applies to all of them
  protected def configFor(topology: Topology): SageConfig = SageConfig(topology = topology, protocol = TestProtocol.selected)

  protected def configOf(server: GenericContainer): SageConfig = configFor(Topology.Standalone(Endpoint(server.host, server.mappedPort(6379))))

  protected def connectAndUse[A](config: SageConfig)(body: Client[CIO, String] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(Client.connect(config))(_.close)(body)

  // SHUTDOWN reports a connection failure because the server closes the socket, so its result is ignored. Later checks verify its effect.
  protected def shutdown(config: SageConfig): CIO[Unit] = connectAndUse(config)(_.run(admin("SHUTDOWN", "NOSAVE"))).liftToTry.unit

  protected def withSubscription[M, A](subscribe: CIO[Subscription[CIO, M]])(body: Subscription[CIO, M] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(subscribe)(_.close)(body)

  // CIO.foreachDiscard with an explicit concurrency does not compile on the Future cell, so one-at-a-time traversals fold instead
  protected def inSequence[A](items: Iterable[A])(f: A => CIO[Any]): CIO[Unit] =
    items.foldLeft(CIO.unit)((previous, item) => previous.flatMap(_ => f(item).unit))

  // Reads `command`'s call count, then gives `next` a step that waits until the count has grown by `by`.
  protected def awaitCalls[A](commandStats: CIO[String], command: String, by: Long)(next: CIO[Unit] => CIO[A]): CIO[A] =
    commandStats.flatMap(before =>
      next(Eventually(30, 50.millis)(commandStats.map(commandCalls(_, command)).satisfies(_ >= commandCalls(before, command) + by)))
    )

  protected def commandCalls(commandStats: String, command: String): Long =
    commandStats.linesIterator
      .find(_.startsWith(s"cmdstat_${command.toLowerCase}:calls="))
      .fold(0L)(_.dropWhile(_ != '=').drop(1).takeWhile(_ != ',').toLong)
}

// Docker Desktop accepts connections on a published port before the server behind it is reachable, so wait for a PING reply through it.
final private class AnswersPing extends AbstractWaitStrategy {

  withRateLimiter(RateLimiterBuilder.newBuilder().withRate(10, TimeUnit.SECONDS).withConstantThroughput().build())

  override protected def waitUntilReady(): Unit =
    waitStrategyTarget.getExposedPorts.forEach { port =>
      val address = new InetSocketAddress(waitStrategyTarget.getHost, waitStrategyTarget.getMappedPort(port))
      Unreliables.retryUntilTrue(startupTimeout.getSeconds.toInt, TimeUnit.SECONDS, () => getRateLimiter.getWhenReady(() => pong(address)))
    }

  private def pong(address: InetSocketAddress): Boolean =
    Using.resource(new Socket()) { socket =>
      socket.connect(address, 1000)
      socket.setSoTimeout(1000)
      socket.getOutputStream.write("PING\r\n".getBytes(US_ASCII))
      new String(socket.getInputStream.readNBytes(7), US_ASCII) == "+PONG\r\n"
    }
}
