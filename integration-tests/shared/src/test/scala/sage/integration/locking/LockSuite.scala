package sage.integration.locking

import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.LockLost
import sage.integration.BothServersSuite

class LockSuite extends BothServersSuite {
  clientsTest("independent clients contend on the same key and acquire after release")((first, second) => contend(first.lock(), second, "shared"))

  clientsTest("waiting scopes protect a read-modify-write operation across clients") { (first, second) =>
    first.set("counter", 0).flatMap { _ =>
      CIO
        .foreach(1 to 12) { i =>
          val client = if (i % 2 == 0) first else second
          client.lock[String]().withLock("counter", 5.seconds) {
            client.get[Int]("counter").flatMap(required("counter", _)).flatMap { current =>
              client.ping().flatMap(_ => client.set("counter", current + 1))
            }
          }
        }
        .flatMap(_ => first.get[Int]("counter"))
        .is(Some(12))
    }
  }

  clientsTest("automatic renewal extends the lease and excludes another client") { (first, second) =>
    first
      .lock[String](leaseDuration = 900.millis)
      .withLock("long", 2.seconds) {
        awaitRenewal(second, "4:lock:long")
          .flatMap(_ => second.lock[String]().tryWithLock("long")(CIO.value(42)))
          .is(None)
      }
      .flatMap(_ => second.lock[String]().tryWithLock("long")(CIO.value(42)))
      .is(Some(42))
  }

  clientsTest("a standalone lock survives connection loss during renewal") { (first, second) =>
    val holder    = first.lock[String](leaseDuration = 1500.millis)
    val contender = second.lock[String]()
    first.clientId.flatMap { id =>
      holder
        .withLock("reconnect", 2.seconds) {
          awaitCalls(second.info("commandstats"), "evalsha", 1)(renewed =>
            second.pipeline((admin("CLIENT", "KILL", "ID", id.toString), admin("CLIENT", "PAUSE", "600", "ALL"))) >> renewed
          )
            .flatMap(_ => contender.tryWithLock("reconnect")(CIO.value(1)))
            .is(None)
        }
        .flatMap(_ => contender.tryWithLock("reconnect")(CIO.value(42)))
        .is(Some(42))
    }
  }

  clientsTest("expired ownership cannot renew or remove a replacement owner's lock") { (first, second) =>
    failsWith[LockLost](
      first.lock[String](leaseDuration = 300.millis).tryWithLock("replaced") {
        second.set("4:lock:replaced", "new-owner").flatMap(_ => CIO.never)
      }
    ).flatMap(_ => second.get[String]("4:lock:replaced").is(Some("new-owner")))
  }

  clientTest("body failure releases the lease and preserves the error") { client =>
    val failure = new IllegalStateException("body failed")
    val lock    = client.lock[String]()
    failsWith[IllegalStateException](lock.withLock("failure", 2.seconds)(CIO.fail(failure))).flatMap { error =>
      assert(error eq failure)
      lock.tryWithLock("failure")(CIO.value(42)).is(Some(42))
    }
  }

  clientTest("script cache flush during a scope is recovered by renewal and release") { client =>
    client
      .scriptFlush()
      .flatMap { _ =>
        client.lock[String](leaseDuration = 900.millis).withLock("flush", 2.seconds) {
          client
            .scriptFlush()
            .flatMap(_ => awaitRenewal(client, "4:lock:flush"))
            .flatMap(_ => client.scriptFlush())
        }
      }
      .flatMap(_ => client.exists("4:lock:flush"))
      .is(0L)
  }

  clientTest("different keys and namespaces remain independent, including through client.as") { client =>
    client
      .lock[String](namespace = "one")
      .tryWithLock("key") {
        for {
          differentKey       <- client.lock[String](namespace = "one").tryWithLock("other")(CIO.value(1))
          differentNamespace <- client.lock[String](namespace = "two").tryWithLock("key")(CIO.value(2))
          same               <- client.as[Array[Byte]].lock[Array[Byte]](namespace = "one").tryWithLock("key".getBytes("UTF-8"))(CIO.value(3))
        } yield (differentKey, differentNamespace, same)
      }
      .is(Some((Some(1), Some(2), None)))
  }
}
