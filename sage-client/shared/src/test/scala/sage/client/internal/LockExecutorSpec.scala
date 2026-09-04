package sage.client.internal

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}

import kyo.compat.*

import sage.Bytes
import sage.SageException.{ConnectionLost, InvalidArgument, LockLost, NotConnected, ServerError, TimedOut}
import sage.client.SageConfig
import sage.commands.Command
import sage.protocol.Frame

class LockExecutorSpec extends munit.FunSuite {
  override val munitTimeout = 10.seconds

  private given ExecutionContext = munitExecutionContext

  test("standalone lock scopes use only the shared connection and do not send ROLE or WAIT") {
    val written                                         = new ConcurrentLinkedQueue[String]()
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      new FakeTransport(
        onFrame,
        onClosed,
        payload => {
          val text = payload.asUtf8String
          written.add(text)
          if (text.contains("HELLO")) Seq(Replies.hello)
          else if (text.contains("EVALSHA")) Seq(Frame.Integer(1))
          else Seq(Replies.ok)
        }
      )
    Client
      .connectWith(factory, Scheduler.real, SageConfig(closeTimeout = Duration.Zero))
      .flatMap { client =>
        CIO.ensure(client.close) {
          client.lock[String]().tryWithLock("key")(CIO.value(42)).map { result =>
            assertEquals(result, Some(42))
            assertEquals(written.asScala.count(_.contains("HELLO")), 1)
            assert(!written.asScala.exists(text => text.contains("ROLE") || text.contains("WAIT")))
          }
        }
      }
      .unsafeRun
  }

  private class Store extends CommandRunner[CIO, String] {
    private var entries                               = Map.empty[String, (String, Long)]
    private var loaded                                = false
    val operations                                    = new ConcurrentLinkedQueue[String]()
    val tokens                                        = new ConcurrentLinkedQueue[String]()
    @volatile var rejectRenewal                       = false
    @volatile var stallRenewal                        = false
    val renewalErrors                                 = new ConcurrentLinkedQueue[Throwable]()
    @volatile var failRelease                         = false
    @volatile var stallRelease                        = false
    @volatile var acquisitionDelay                    = Duration.Zero
    @volatile var acquisitionFailureDelay             = Duration.Zero
    @volatile var acquisitionError: Option[Throwable] = None
    val acquisitionErrors                             = new ConcurrentLinkedQueue[Throwable]()
    val acquisitionReplyErrors                        = new ConcurrentLinkedQueue[Throwable]()
    private var renewed                               = false
    private var renewalWaiter: Try[Unit] => Unit      = null

    def awaitRenewal: CIO[Unit] = CIO.async { callback =>
      val completeNow = synchronized {
        if (renewed) true
        else {
          renewalWaiter = callback
          false
        }
      }
      if (completeNow) callback(Success(()))
    }

    private def markRenewed(): Unit = {
      val callback = synchronized {
        renewed = true
        val waiting = renewalWaiter
        renewalWaiter = null
        waiting
      }
      if (callback != null) callback(Success(()))
    }

    def held: Boolean = synchronized(entries.values.exists(_._2 > System.nanoTime()))

    def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
      assertEquals(command.keyIndices, Vector(2))
      assert(!command.isReadOnly && !command.cacheable && !command.allMasters)
      val operation = command.args(4).asUtf8String
      val key       = command.args(2).asUtf8String
      val token     = command.args(3).asUtf8String
      operations.add(s"${command.name}:$operation")
      if (operation == "renew" && stallRenewal) CIO.never
      else if (operation == "renew" && !renewalErrors.isEmpty) CIO.fail(renewalErrors.remove())
      else if (operation == "release" && stallRelease) CIO.never
      else if (operation == "release" && failRelease) CIO.fail(ServerError("ERR", "release unavailable"))
      else if (operation == "acquire" && !acquisitionErrors.isEmpty) {
        val error = acquisitionErrors.remove()
        CIO.sleep(acquisitionFailureDelay).flatMap(_ => CIO.fail(error))
      } else if (operation == "acquire" && acquisitionError.isDefined) CIO.fail(acquisitionError.get)
      else {
        val result = synchronized {
          if (command.name == "EVALSHA" && !loaded) Left(ServerError("NOSCRIPT", ""))
          else {
            loaded = true
            val now     = System.nanoTime()
            entries = entries.filter(_._2._2 > now)
            val owns    = entries.get(key).exists(_._1 == token)
            val expiry  = now + command.args(5).asUtf8String.toLong.millis.toNanos
            val success = operation match {
              case "acquire" if !entries.contains(key) =>
                entries += key -> (token, expiry)
                tokens.add(token)
                true
              case "acquire" if owns                   =>
                entries += key -> (token, expiry)
                true
              case "renew" if owns && !rejectRenewal   =>
                entries += key -> (token, expiry)
                markRenewed()
                true
              case "release" if owns                   =>
                entries -= key
                true
              case _                                   => false
            }
            command.decode(Frame.Integer(if (success) 1L else 0L))
          }
        }
        val delay  = if (operation == "acquire" && command.name == "EVAL") acquisitionDelay else Duration.Zero
        CIO.sleep(delay).flatMap { _ =>
          if (operation == "acquire" && result.toOption.contains(true) && !acquisitionReplyErrors.isEmpty)
            CIO.fail(acquisitionReplyErrors.remove())
          else result.fold(CIO.fail(_), CIO.value(_))
        }
      }
    }
  }

  private def executor(lease: FiniteDuration = 3.seconds, namespace: String = "lock") =
    new LockExecutor[String](lease, namespace, replicaAcknowledgement = true)

  test("namespace framing distinguishes ambiguous prefixes and preserves binary key bytes") {
    val a      = new LockCommands[String](1.second, "a").key("b:c")
    val b      = new LockCommands[String](1.second, "a:b").key("c")
    assert(!a.sameBytes(b))
    val raw    = Array[Byte](0, -1, 42)
    val binary = new LockCommands[Array[Byte]](1.second, "é").key(raw)
    assert(binary.sameBytes(Bytes.concat(Vector(Bytes.utf8("2:é:"), Bytes.fromArray(raw)))))
  }

  test("all scripts route to one key on its master and decode only valid outcomes") {
    val commands = new LockCommands[String](1.second, "lock")
    for (operation <- LockCommands.Operation.values) {
      val command = commands.command(commands.key("subject"), "owner", operation, cached = true)
      assertEquals(command.name, "EVALSHA")
      assertEquals(command.keys.map(_.asUtf8String), Vector("4:lock:subject"))
      assert(!command.cacheable && !command.isReadOnly && !command.isBlocking && !command.allMasters)
      assertEquals(command.decode(Frame.Integer(0)), Right(false))
      assertEquals(command.decode(Frame.Integer(1)), Right(true))
      assert(command.decode(Frame.Integer(2)).isLeft)
      assert(command.decode(Frame.Null).isLeft)
    }
  }

  test("contended tryWithLock never evaluates the body, and successful scopes release") {
    val store     = new Store
    val lock      = executor()
    var evaluated = false
    lock
      .tryWithLock(store, "key") {
        lock.tryWithLock(store, "key") {
          evaluated = true
          CIO.value(42)
        }
      }
      .unsafeRun
      .map { result =>
        assertEquals(result, Some(None))
        assert(!evaluated)
        assert(!store.held)
      }
  }

  test("a successful renewal keeps the scope running and releases it afterwards") {
    val store = new Store
    executor(300.millis)
      .tryWithLock(store, "key") {
        store.awaitRenewal.map(_ => assert(store.held))
      }
      .unsafeRun
      .map { result =>
        assertEquals(result, Some(()))
        assert(!store.held)
        assert(store.operations.asScala.exists(_.endsWith(":renew")))
      }
  }

  test("a slow successful acquisition renews before the remaining lease expires") {
    val store = new Store
    store.acquisitionDelay = 500.millis
    executor(900.millis)
      .tryWithLock(store, "key") {
        store.awaitRenewal.map(_ => assert(store.held, "the lock expired while its body was still running"))
      }
      .unsafeRun
      .map { result =>
        assertEquals(result, Some(()))
        assert(store.operations.asScala.exists(_.endsWith(":renew")))
        assert(!store.held)
      }
  }

  for ((failureName, failure) <- Vector("connection failure" -> NotConnected(), "operation timeout" -> TimedOut("lock write timed out")))
    test(s"renewal retries a transient $failureName while the lease remains valid") {
      val store = new Store
      store.renewalErrors.add(failure)
      executor(300.millis)
        .tryWithLock(store, "key") {
          store.awaitRenewal.map(_ => assert(store.held, "the lock expired while renewal was retrying"))
        }
        .unsafeRun
        .map { result =>
          assertEquals(result, Some(()))
          assert(store.operations.asScala.count(_.endsWith(":renew")) >= 2)
          assert(!store.held)
        }
    }

  test("renewal refusal fails even while the protected effect never succeeds") {
    val store = new Store
    store.rejectRenewal = true
    executor(300.millis).tryWithLock(store, "key")(CIO.never).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[LockLost], error.toString)
      assert(!store.held)
    }
  }

  test("an unresponsive renewal is bounded by the remaining lease") {
    val store = new Store
    store.stallRenewal = true
    executor(300.millis).tryWithLock(store, "key")(CIO.never).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[LockLost], error.toString)
      assert(!store.held)
    }
  }

  test("a renewal acknowledgement shortfall becomes LockLost and releases ownership") {
    val shortfall = LockReplication.acknowledgementTimedOut("replication confirmed by 0 of 1 required replicas")
    val store     = new Store {
      override def lockWrite(
        command: Command[Boolean],
        timeout: FiniteDuration,
        replicaAcknowledgement: Boolean
      ): CIO[Boolean] =
        run(command).flatMap { renewed =>
          if (command.args(4).asUtf8String == "renew") CIO.fail(shortfall)
          else CIO.value(renewed)
        }
    }
    executor(300.millis).tryWithLock(store, "key")(CIO.never).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[LockLost], error.toString)
      assert(error.getCause eq shortfall)
      assertEquals(store.operations.asScala.count(_.endsWith(":renew")), 1)
      assert(store.operations.asScala.exists(_.endsWith(":release")))
      assert(!store.held)
    }
  }

  test("replication checks respect a short acquisition wait and the remaining renewal lease") {
    val store = new Store {
      override def lockWrite(
        command: Command[Boolean],
        timeout: FiniteDuration,
        replicaAcknowledgement: Boolean
      ): CIO[Boolean] = {
        val operation = command.args(4).asUtf8String
        if (operation == "acquire") assert(timeout > Duration.Zero && timeout <= 200.millis, timeout.toString)
        if (operation == "renew") assert(timeout > Duration.Zero && timeout <= 170.millis, timeout.toString)
        run(command)
      }
    }
    executor(300.millis).withLock(store, "key", 200.millis)(store.awaitRenewal).unsafeRun.map { _ =>
      assert(store.operations.asScala.exists(_.endsWith(":renew")))
      assert(!store.held)
    }
  }

  test("a body error survives cleanup failure") {
    val store   = new Store
    store.failRelease = true
    val failure = new IllegalStateException("body failed")
    executor().tryWithLock(store, "key")(CIO.fail(failure)).unsafeRun.failed.map { error =>
      assert(error eq failure)
      assert(store.operations.asScala.exists(_.endsWith(":release")))
    }
  }

  test("a successful body does not hide release failure") {
    val store = new Store
    store.failRelease = true
    executor().tryWithLock(store, "key")(CIO.value(42)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], error.toString)
    }
  }

  test("checked release and fallback cleanup share one time budget") {
    val store   = new Store
    store.stallRelease = true
    val started = System.nanoTime()
    executor(300.millis).tryWithLock(store, "key")(CIO.value(42)).unsafeRun.failed.map { error =>
      val elapsed = (System.nanoTime() - started).nanos
      assert(error.isInstanceOf[LockLost], error.toString)
      assert(elapsed < 2.seconds, s"release exceeded its shared budget: $elapsed")
      assertEquals(store.operations.asScala.count(_.endsWith(":release")), 1)
    }
  }

  test("withLock times out under contention without evaluating its body") {
    val store     = new Store
    val lock      = executor()
    var evaluated = false
    lock
      .tryWithLock(store, "key") {
        lock
          .withLock(store, "key", 100.millis) {
            evaluated = true
            CIO.unit
          }
          .liftToTry
      }
      .unsafeRun
      .map { result =>
        assert(result.get.failed.get.isInstanceOf[TimedOut])
        assert(!evaluated)
        assert(!store.held)
      }
  }

  test("a delayed acquisition never starts the body and attempts ownership-checked cleanup") {
    val store     = new Store
    store.acquisitionDelay = 400.millis
    var evaluated = false
    executor()
      .withLock(store, "key", 100.millis) {
        evaluated = true
        CIO.unit
      }
      .unsafeRun
      .failed
      .map { error =>
        assert(error.isInstanceOf[TimedOut], error.toString)
        assert(!evaluated)
        assert(!store.held)
      }
  }

  test("sustained contention reduces acquisition requests while respecting the wait timeout") {
    val attempts = new AtomicInteger(0)
    val commands = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
        attempts.incrementAndGet()
        command.decode(Frame.Integer(0)).fold(CIO.fail(_), CIO.value(_))
      }
    }
    val started  = System.nanoTime()
    executor().withLock(commands, "key", 200.millis)(CIO.fail(new AssertionError("contended body ran"))).unsafeRun.failed.map { error =>
      val elapsed = (System.nanoTime() - started).nanos
      assert(error.isInstanceOf[TimedOut], error.toString)
      assert(attempts.get() >= 2, s"acquisition was not retried: ${attempts.get()}")
      assert(attempts.get() <= 10, s"contention generated too many acquisition requests: ${attempts.get()}")
      assert(elapsed >= 180.millis && elapsed < 1.second, s"wait timeout was not respected: $elapsed")
    }
  }

  test("withLock retries a transient acquisition failure within the wait timeout") {
    val store = new Store
    store.acquisitionErrors.add(NotConnected())
    executor().withLock(store, "key", 1.second)(CIO.value(42)).unsafeRun.map { result =>
      assertEquals(result, 42)
      assert(store.operations.asScala.count(_.endsWith(":acquire")) >= 2)
      assert(!store.held)
    }
  }

  test("withLock keeps retrying transient acquisition failures when the wait is longer than the lease") {
    val store = new Store
    store.acquisitionFailureDelay = 150.millis
    store.acquisitionErrors.add(NotConnected())
    store.acquisitionErrors.add(NotConnected())
    executor(300.millis).withLock(store, "key", 1.second)(CIO.value(42)).unsafeRun.map { result =>
      assertEquals(result, 42)
      assert(store.operations.asScala.count(_.endsWith(":acquire")) >= 3)
      assert(!store.held)
    }
  }

  test("tryWithLock caps transient acquisition retries at the operation timeout") {
    val store   = new Store
    store.acquisitionError = Some(NotConnected())
    val started = System.nanoTime()
    executor(300.millis).tryWithLock(store, "key")(CIO.value(42)).unsafeRun.failed.map { error =>
      val elapsed = (System.nanoTime() - started).nanos
      assert(error.isInstanceOf[TimedOut], error.toString)
      assert(store.operations.asScala.count(_.endsWith(":acquire")) >= 2)
      assert(elapsed >= 240.millis && elapsed < 1.second, s"tryWithLock did not use its operation timeout: $elapsed")
    }
  }

  test("an acquisition retry reuses its ownership token after an uncertain reply") {
    val store = new Store
    store.acquisitionReplyErrors.add(ConnectionLost(mayHaveExecuted = true))
    executor().withLock(store, "key", 1.second)(CIO.value(42)).unsafeRun.map { result =>
      assertEquals(result, 42)
      assertEquals(store.tokens.asScala.size, 1)
      assert(store.operations.asScala.count(_.endsWith(":acquire")) >= 3)
      assert(!store.held)
    }
  }

  test("NOSCRIPT falls back to EVAL once and subsequent operations use the digest") {
    val store = new Store
    executor().tryWithLock(store, "key")(CIO.value(42)).unsafeRun.map { result =>
      assertEquals(result, Some(42))
      assertEquals(store.operations.asScala.toList, List("EVALSHA:acquire", "EVAL:acquire", "EVALSHA:release"))
    }
  }

  test("retryable acquisition errors are retried until the wait timeout") {
    val store   = new Store
    val failure = ServerError("READONLY", "demoted")
    store.acquisitionError = Some(failure)
    executor().withLock(store, "key", 200.millis)(CIO.unit).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[TimedOut], error.toString)
      assert(store.operations.asScala.count(_.endsWith(":acquire")) >= 2)
    }
  }

  test("non-retryable acquisition errors propagate") {
    val store   = new Store
    val failure = ServerError("ERR", "lock command is not permitted")
    store.acquisitionError = Some(failure)
    executor().withLock(store, "key", 1.second)(CIO.unit).unsafeRun.failed.map { error =>
      assertEquals(error, failure)
      assertEquals(store.operations.asScala.count(_.endsWith(":acquire")), 1)
    }
  }

  test("reevaluating a lock effect generates a fresh owner token") {
    val store  = new Store
    val effect = executor().tryWithLock(store, "key")(CIO.value(42))
    effect.flatMap(_ => effect).unsafeRun.map { _ =>
      assertEquals(store.tokens.size(), 2)
      assertEquals(store.tokens.asScala.toSet.size, 2)
    }
  }

  test("invalid lease or wait duration fails before any server command") {
    val store = new Store
    for {
      lease <- executor(1.millis).tryWithLock(store, "key")(CIO.unit).unsafeRun.failed
      wait  <- executor().withLock(store, "key", Duration.Zero)(CIO.unit).unsafeRun.failed
    } yield {
      assert(lease.isInstanceOf[InvalidArgument])
      assert(wait.isInstanceOf[InvalidArgument])
      assert(store.operations.isEmpty)
    }
  }
}
