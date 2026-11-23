package sage.integration

import scala.concurrent.duration.*
import scala.reflect.{classTag, ClassTag}
import scala.util.Failure

import kyo.compat.*
import munit.Location

import sage.Bytes
import sage.client.LockClient
import sage.client.internal.{Client, Paged}
import sage.commands.{Command, Commands}

/**
  * Assertion and setup steps for the command suites. They do not start containers, so the in-memory conformance suite compiles this file
  * too and runs the same suites against the in-memory client.
  */
trait ClientAssertions extends munit.FunSuite {

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
}
