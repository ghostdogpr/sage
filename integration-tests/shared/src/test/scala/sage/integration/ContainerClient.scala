package sage.integration

import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Using}

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainersSuite
import kyo.compat.*
import munit.{Location, TestOptions}
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder
import org.rnorth.ducttape.unreliables.Unreliables
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy

import sage.Bytes
import sage.client.{Endpoint, LockClient, SageConfig, Topology}
import sage.client.internal.{Client, Paged, Subscription}
import sage.commands.{Command, Commands}

/**
  * Shared connect-and-teardown helpers for the testcontainers suites: build a config for a started container, and run a body against a
  * client that is always closed afterwards.
  */
trait ContainerClient extends munit.FunSuite with TestContainersSuite {

  // The Ox cell's unsafeRun uses this value. Keeping it non-private avoids unused-private warnings in the other cells.
  given ExecutionContext = munitExecutionContext

  protected def serverDef(image: String, exposedPorts: Seq[Int] = Seq(6379), command: Seq[String] = Seq()): GenericContainer.Def[GenericContainer] =
    GenericContainer.Def(image, exposedPorts = exposedPorts, command = command, waitStrategy = new AnswersPing)

  // container hooks run outside munitTimeout
  protected def prepare(setup: CIO[Unit]): Unit = Await.result(setup.unsafeRun, 3.minutes)

  protected def containerTest(options: TestOptions)(body: Containers => CIO[Any])(using Location): Unit =
    test(options)(withContainers(body(_).unsafeRun))

  protected def configOf(server: GenericContainer): SageConfig =
    SageConfig(topology = Topology.Standalone(Endpoint(server.host, server.mappedPort(6379))))

  protected def connectAndUse[A](config: SageConfig)(body: Client[CIO, String] => CIO[A]): CIO[A] = opened(Client.connect(config))(_.close)(body)

  // SHUTDOWN reports a connection failure because the server closes the socket, so its result is ignored. Later checks verify its effect.
  protected def shutdown(config: SageConfig): CIO[Unit] = connectAndUse(config)(_.run(admin("SHUTDOWN", "NOSAVE"))).liftToTry.unit

  protected def withSubscription[M, A](subscribe: CIO[Subscription[CIO, M]])(body: Subscription[CIO, M] => CIO[A]): CIO[A] =
    opened(subscribe)(_.close)(body)

  // CIO.acquireReleaseWith fails to compile on the Ox/Future cells when its type argument nests CIO (Client[CIO, String]); fold instead
  private def opened[R, A](open: CIO[R])(close: R => CIO[Unit])(body: R => CIO[A]): CIO[A] =
    open.flatMap(resource => body(resource).fold(result => close(resource).map(_ => result), error => close(resource).flatMap(_ => CIO.fail(error))))

  extension [A](call: CIO[A]) {
    protected def is(expected: A)(using Location): CIO[Unit]                = call.flatMap(value => CIO.defer(assertEquals(value, expected)))
    protected def satisfies(holds: A => Boolean)(using Location): CIO[Unit] = call.flatMap(value => CIO.defer(assert(holds(value), value)))
    protected def >>[B](next: CIO[B]): CIO[B]                               = call.flatMap(_ => next)
  }

  protected def failsWith[E <: Throwable: ClassTag](attempt: CIO[Any]): CIO[E] =
    attempt.liftToTry.flatMap {
      case Failure(error: E) => CIO.value(error)
      case other             => CIO.fail(new AssertionError(s"expected ${classTag[E].runtimeClass.getSimpleName}, got $other"))
    }

  protected def drain[S, A](pages: Paged.Pages[S, A]): CIO[Set[A]] = {
    def loop(state: S, found: Set[A]): CIO[Set[A]] =
      pages.step(state).flatMap {
        case Some((items, next)) => loop(next, found ++ items)
        case None                => CIO.value(found)
      }
    loop(pages.init, Set.empty)
  }

  // CIO.foreachDiscard with an explicit concurrency does not compile on the Future cell, so one-at-a-time traversals fold instead
  protected def inSequence[A](items: Iterable[A])(f: A => CIO[Any]): CIO[Unit] =
    items.foldLeft(CIO.unit)((previous, item) => previous.flatMap(_ => f(item).unit))

  protected def required[A](what: String, value: Option[A]): CIO[A] =
    CIO.get(value.toRight(new AssertionError(s"$what returned nothing")).toTry)

  // For server commands the library does not model, such as REPLICAOF, CLIENT PAUSE, and SHUTDOWN. The reply is ignored.
  protected def admin(name: String, args: String*): Command[Unit] =
    Command(name, Command.NoKeys, args.toVector.map(Bytes.utf8), _ => Right(()))

  // The writer uses a separate connection, so each write is a server-side change for the reader's cache.
  protected def cachedReadIsInvalidated(reader: Client[CIO, String], writer: Client[CIO, String], key: String): CIO[Unit] = {
    val read = reader.cached(Commands.get[String, String](key), 1.minute)
    writer.set(key, "v1") >>
      read.is(Some("v1")) >>
      read.is(Some("v1")) >>
      writer.set(key, "v2") >>
      // a cached read does not contact the server again. Poll until the invalidation message from an external write has been processed.
      Eventually(50)(read.is(Some("v2")))
  }

  protected def contend(
    holder: LockClient[CIO, String],
    contender: Client[CIO, String],
    key: String,
    whileHeld: CIO[Unit] = CIO.unit
  ): CIO[Unit] = {
    val waiting = contender.lock[String]()
    holder
      .withLock(key, 2.seconds)(whileHeld.flatMap(_ => waiting.tryWithLock(key)(CIO.fail(new AssertionError("contended body ran")))))
      .is(None) >>
      waiting.tryWithLock(key)(CIO.value(42)).is(Some(42)) >>
      contender.exists(s"4:lock:$key").is(0L)
  }

  protected def awaitRenewal(client: Client[CIO, String], key: String): CIO[Unit] = {
    val ttl = client.pTtl(key)
    Eventually(30, Duration.Zero)(ttl.flatMap(before => CIO.sleep(50.millis).flatMap(_ => ttl).satisfies(Ttls.renewed(before, _))))
  }

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
