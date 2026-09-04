package sage.integration.locking

import scala.concurrent.Future
import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.LockLost
import sage.client.internal.Client
import sage.commands.Command
import sage.integration.{Images, ServerSuite}

abstract class LockSuite(image: String) extends ServerSuite(image) {
  private def killClient(id: Long): Command[Unit] =
    Command(
      "CLIENT",
      Command.NoKeys,
      Vector("KILL", "ID", id.toString).map(Bytes.utf8),
      _ => Right(())
    )

  private val pauseClients: Command[Unit] =
    Command(
      "CLIENT",
      Command.NoKeys,
      Vector("PAUSE", "450", "ALL").map(Bytes.utf8),
      _ => Right(())
    )

  private def withClients[A](body: (Client[CIO, String], Client[CIO, String]) => CIO[A]): Future[A] =
    withContainers { server =>
      connectAndUse(configOf(server)) { first =>
        connectAndUse(configOf(server))(second => body(first, second))
      }.unsafeRun
    }

  test("independent clients contend on the same key and acquire after release") {
    withClients { (first, second) =>
      val a = first.lock[String]()
      val b = second.lock[String]()
      for {
        denied   <- a.withLock("shared", 2.seconds) {
                      b.tryWithLock("shared")(CIO.fail(new AssertionError("contended body ran")))
                    }
        acquired <- b.tryWithLock("shared")(CIO.value(42))
        exists   <- first.exists("4:lock:shared")
      } yield {
        assertEquals(denied, None)
        assertEquals(acquired, Some(42))
        assertEquals(exists, 0L)
      }
    }
  }

  test("waiting scopes protect a read-modify-write operation across clients") {
    withClients { (first, second) =>
      first.set("counter", 0).flatMap { _ =>
        CIO
          .foreach(1 to 12) { i =>
            val client = if (i % 2 == 0) first else second
            client.lock[String]().withLock("counter", 5.seconds) {
              client.get[Int]("counter").flatMap { current =>
                CIO.sleep(5.millis).flatMap(_ => client.set("counter", current.get + 1))
              }
            }
          }
          .flatMap(_ => first.get[Int]("counter"))
          .map(value => assertEquals(value, Some(12)))
      }
    }
  }

  test("automatic renewal excludes another client after the initial lease would have expired") {
    withClients { (first, second) =>
      first
        .lock[String](leaseDuration = 600.millis)
        .withLock("long", 2.seconds) {
          CIO.sleep(1500.millis).flatMap { _ =>
            second.lock[String]().tryWithLock("long")(CIO.value(42)).map(result => assertEquals(result, None))
          }
        }
        .flatMap(_ => second.lock[String]().tryWithLock("long")(CIO.value(42)))
        .map(result => assertEquals(result, Some(42)))
    }
  }

  test("a standalone lock survives connection loss during renewal") {
    withClients { (first, second) =>
      val holder    = first.lock[String](leaseDuration = 1500.millis)
      val contender = second.lock[String]()
      first.clientId.flatMap { id =>
        holder
          .withLock("reconnect", 2.seconds) {
            CIO
              .sleep(250.millis)
              .flatMap(_ => second.pipeline((killClient(id), pauseClients)))
              .flatMap(_ => CIO.sleep(1400.millis))
              .flatMap(_ => contender.tryWithLock("reconnect")(CIO.value(1)))
              .map(result => assertEquals(result, None))
          }
          .flatMap(_ => contender.tryWithLock("reconnect")(CIO.value(42)))
          .map(result => assertEquals(result, Some(42)))
      }
    }
  }

  test("expired ownership cannot renew or remove a replacement owner's lock") {
    withClients { (first, second) =>
      first
        .lock[String](leaseDuration = 300.millis)
        .tryWithLock("replaced") {
          second.set("4:lock:replaced", "new-owner").flatMap(_ => CIO.never)
        }
        .liftToTry
        .flatMap { result =>
          assert(result.failed.get.isInstanceOf[LockLost], result.toString)
          second.get[String]("4:lock:replaced").map(value => assertEquals(value, Some("new-owner")))
        }
    }
  }

  test("body failure releases the lease and preserves the error") {
    withClient { client =>
      val failure = new IllegalStateException("body failed")
      val lock    = client.lock[String]()
      lock.withLock("failure", 2.seconds)(CIO.fail(failure)).liftToTry.flatMap { result =>
        assert(result.failed.get eq failure)
        lock.tryWithLock("failure")(CIO.value(42)).map(value => assertEquals(value, Some(42)))
      }
    }
  }

  test("script cache flush during a scope is recovered by renewal and release") {
    withClient { client =>
      client
        .scriptFlush()
        .flatMap { _ =>
          client.lock[String](leaseDuration = 600.millis).withLock("flush", 2.seconds) {
            client.scriptFlush().flatMap(_ => CIO.sleep(1.second)).flatMap(_ => client.scriptFlush())
          }
        }
        .flatMap(_ => client.exists("4:lock:flush"))
        .map(count => assertEquals(count, 0L))
    }
  }

  test("different keys and namespaces remain independent, including through client.as") {
    withClient { client =>
      client
        .lock[String](namespace = "one")
        .tryWithLock("key") {
          for {
            differentKey       <- client.lock[String](namespace = "one").tryWithLock("other")(CIO.value(1))
            differentNamespace <- client.lock[String](namespace = "two").tryWithLock("key")(CIO.value(2))
            same               <- client.as[Array[Byte]].lock[Array[Byte]](namespace = "one").tryWithLock("key".getBytes("UTF-8"))(CIO.value(3))
          } yield {
            assertEquals(differentKey, Some(1))
            assertEquals(differentNamespace, Some(2))
            assertEquals(same, None)
          }
        }
        .unit
    }
  }
}

class RedisLockSuite  extends LockSuite(Images.redis)
class ValkeyLockSuite extends LockSuite(Images.valkey)
