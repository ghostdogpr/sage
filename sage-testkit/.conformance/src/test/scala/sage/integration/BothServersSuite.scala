package sage.integration

import scala.annotation.unused
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.reflect.{classTag, ClassTag}
import scala.util.Failure

import kyo.compat.*
import munit.{Location, TestOptions}

import sage.Bytes
import sage.client.LockClient
import sage.client.SageConfig
import sage.client.inmemory.InMemoryClient
import sage.client.internal.{Client, Paged, Subscription}
import sage.commands.{Command, Commands}
import sage.testkit.InMemoryServer

/**
  * Stands in for the integration tests' `BothServersSuite` so that their command suites run unchanged against the in-memory client. Every
  * test gets a new client with an empty keyspace. Tests written for only Redis or only Valkey run too, because the in-memory client aims
  * to behave like both.
  */
abstract class BothServersSuite extends munit.FunSuite {

  given ExecutionContext = munitExecutionContext

  protected def clientTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    if (BothServersSuite.unsupported.exists(options.name.startsWith(_))) test(options.ignore)(())
    else
      test(options) {
        val client = InMemoryClient()
        CIO.ensure(client.close)(body(client)).unsafeRun
      }

  // two clients of one keyspace, like two connections to the same server
  protected def clientsTest(options: TestOptions)(body: (Client[CIO, String], Client[CIO, String]) => CIO[Any])(using Location): Unit =
    if (BothServersSuite.unsupported.exists(options.name.startsWith(_))) test(options.ignore)(())
    else
      test(options) {
        val server = InMemoryServer()
        val first  = InMemoryClient(server)
        val second = InMemoryClient(server)
        CIO.ensure(first.close.flatMap(_ => second.close))(body(first, second)).unsafeRun
      }

  // The container hooks below exist so that suites mixing server-level tests with ordinary ones compile. Tests that use them are
  // listed as unsupported, so their bodies never run.
  protected def serverTest(options: TestOptions)(@unused body: Nothing => CIO[Any])(using Location): Unit = test(options.ignore)(())

  protected def configOf(@unused server: Any): SageConfig = SageConfig()

  protected def connectAndUse[A](@unused config: SageConfig)(body: Client[CIO, String] => CIO[A]): CIO[A] = {
    val client = InMemoryClient()
    CIO.ensure(client.close)(body(client))
  }

  protected def cachedReadIsInvalidated(reader: Client[CIO, String], writer: Client[CIO, String], key: String): CIO[Unit] = {
    val read = reader.cached(Commands.get[String, String](key), 1.minute)
    writer.set(key, "v1") >>
      read.is(Some("v1")) >>
      read.is(Some("v1")) >>
      writer.set(key, "v2") >>
      Eventually(50)(read.is(Some("v2")))
  }

  protected def redisTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit = clientTest(options)(body)

  protected def valkeyTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit = clientTest(options)(body)

  // the helpers below are copied from the integration tests' ContainerClient, which also starts containers

  extension [A](call: CIO[A]) {
    protected def is(expected: A)(using Location): CIO[Unit]                = call.flatMap(value => CIO.defer(assertEquals(value, expected)))
    protected def satisfies(holds: A => Boolean)(using Location): CIO[Unit] = call.flatMap(value => CIO.defer(assert(holds(value), value)))
    protected def >>[B](next: CIO[B]): CIO[B]                               = call.flatMap(_ => next)
  }

  protected def drain[S, A](pages: Paged.Pages[S, A]): CIO[Set[A]] = {
    def loop(state: S, found: Set[A]): CIO[Set[A]] =
      pages.step(state).flatMap {
        case Some((items, next)) => loop(next, found ++ items)
        case None                => CIO.value(found)
      }
    loop(pages.init, Set.empty)
  }

  protected def required[A](what: String, value: Option[A]): CIO[A] =
    CIO.get(value.toRight(new AssertionError(s"$what returned nothing")).toTry)

  protected def withSubscription[M, A](subscribe: CIO[Subscription[CIO, M]])(body: Subscription[CIO, M] => CIO[A]): CIO[A] =
    subscribe.flatMap(sub => body(sub).fold(result => sub.close.map(_ => result), error => sub.close.flatMap(_ => CIO.fail(error))))

  protected def failsWith[E <: Throwable: ClassTag](attempt: CIO[Any]): CIO[E] =
    attempt.liftToTry.flatMap {
      case Failure(error: E) => CIO.value(error)
      case other             => CIO.fail(new AssertionError(s"expected ${classTag[E].runtimeClass.getSimpleName}, got $other"))
    }

  protected def admin(name: String, args: String*): Command[Unit] =
    Command(name, Command.NoKeys, args.toVector.map(Bytes.utf8), _ => Right(()))

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

  // only the ignored connection-loss test uses this, so it does not wait for anything
  protected def awaitCalls[A](commandStats: CIO[String], @unused command: String, @unused by: Long)(next: CIO[Unit] => CIO[A]): CIO[A] =
    commandStats.flatMap(_ => next(CIO.unit))
}

object BothServersSuite {

  /**
    * Tests of commands that the in-memory client deliberately does not implement, by the start of the test name. They are reported as
    * ignored. `DIGEST` needs the server's XXH3 hash, `MOVE` needs several databases, `DUMP`, `RESTORE` and `MIGRATE` need the server's
    * serialization format, and `OBJECT` and `CONFIG` describe server internals. The in-memory client also has no connections that can be
    * lost.
    */
  val unsupported: Set[String] = Set(
    "DIGEST returns",
    "DELEX deletes only when the value or digest condition matches",
    "MOVE relocates",
    "DUMP and RESTORE",
    "MIGRATE reports",
    "OBJECT exposes",
    "OBJECT FREQ",
    // the in-memory client has no connections to kill, and no INFO
    "a standalone lock survives connection loss",
    "closing the client releases its server connection"
  )
}
