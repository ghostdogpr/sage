package sage.client.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.{LockLost, TimedOut}
import sage.backend.SageClient
import sage.client.{DedicatedPoolConfig, MasterReplicaConfig, SageConfig}
import sage.cluster.Node
import sage.commands.{Command, Connection, Execution}
import sage.protocol.Frame

class LockFutureSpec extends munit.FunSuite {
  private given ExecutionContext = munitExecutionContext

  test("a confirmation timeout frees the dedicated connection when its Future never receives a reply") {
    val master    = Node("master", 6379)
    val replica   = Node("replica", 6380)
    val evaluated = new AtomicBoolean(false)
    val waiting   = new AtomicBoolean(false)
    val live      = new MasterReplicaLive(
      node =>
        (onFrame, onClosed) =>
          new FakeTransport(
            onFrame,
            onClosed,
            payload => {
              val text = payload.asUtf8String
              if (text.contains("HELLO")) Seq(Replies.hello)
              else if (text.contains("ROLE")) Seq(if (node == master) Replies.masterRole(replica) else Replies.replicaRole(master))
              else if (text.contains("WAIT")) { waiting.set(true); Nil }
              else if (text.contains("PING")) Seq(Frame.SimpleString("PONG"))
              else if (text.contains("EVALSHA")) Seq(Frame.Integer(1))
              else Seq(Replies.ok)
            }
          ),
      Scheduler.real,
      Vector(Connection.hello()),
      SageConfig(dedicatedPool = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 200.millis), closeTimeout = Duration.Zero),
      Vector(master),
      MasterReplicaConfig()
    )
    live.bootstrapRoles()
    val client    = new SageClient.Lowered(live)
    val checked   = for {
      error <- client
                 .lock[String](300.millis)
                 .tryWithLock("key") {
                   evaluated.set(true)
                   scala.concurrent.Future.successful(42)
                 }
                 .failed
      _      = assert(error.isInstanceOf[TimedOut], error.toString)
      _      = assert(waiting.get(), "the test did not reach replication confirmation")
      _      = assert(!evaluated.get())
      pong  <- client.run(Connection.ping().copy(execution = Execution.Blocking))
    } yield assertEquals(pong, "PONG")
    checked.andThen { case _ => live.close.unsafeRun }
  }

  test("lease loss fails the scope while a Future body keeps running") {
    val body      = Promise[Int]()
    val continued = Promise[Int]()
    val released  = new AtomicBoolean(false)
    val commands  = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
        val reply = command.args(4).asUtf8String match {
          case "renew"   =>
            0L
          case "release" =>
            released.set(true)
            1L
          case _         => 1L
        }
        command.decode(Frame.Integer(reply)).fold(CIO.fail(_), CIO.value(_))
      }
    }
    val client    = new SageClient.Lowered(new LockTestClient(commands))
    val scoped    = client.lock[String](300.millis).tryWithLock("key") {
      body.future.map { value =>
        continued.success(value)
        value
      }
    }
    for {
      error <- scoped.failed
      _      = assert(error.isInstanceOf[LockLost], error.toString)
      _      = assert(!continued.isCompleted)
      _      = assert(released.get())
      _      = body.success(42)
      value <- continued.future
    } yield assertEquals(value, 42)
  }
}
