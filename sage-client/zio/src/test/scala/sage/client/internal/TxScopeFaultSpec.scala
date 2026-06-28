package sage.client.internal

import scala.collection.mutable
import scala.concurrent.ExecutionContext

import kyo.compat.*

import sage.Bytes
import sage.SageException.{ConnectionLost, ServerError}
import sage.client.DedicatedPoolConfig
import sage.commands.{Connection, Strings}
import sage.protocol.Frame

class TxScopeFaultSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private val readonly = Frame.SimpleError("READONLY You can't write against a read only replica.")

  private def txScope(respond: Bytes => Seq[Frame]): (Client.TxScope, mutable.ArrayBuffer[Throwable]) = {
    val scheduler                                       = new ManualScheduler
    val gen                                             = MultiplexedConnection.Generation.initial
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respond)
    val pool                                            =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, () => true, () => Some(gen), _ == gen, DedicatedPoolConfig(), 1000L)
    val faults                                          = mutable.ArrayBuffer.empty[Throwable]
    (new Client.TxScope(pool.acquireForTransaction(), faults += _), faults)
  }

  private def isOwnershipFault(error: Throwable): Boolean = error match {
    case e: ServerError    => e.code == "READONLY"
    case _: ConnectionLost => true
    case _                 => false
  }

  test("a READONLY command fault invokes the onFault hook") {
    val (scope, faults) = txScope(p => if (p.asUtf8String.contains("HELLO")) Seq(helloReply) else Seq(readonly))
    scope.run(Strings.set("k", "v")).unsafeRun.failed.map { _ =>
      assert(faults.exists(isOwnershipFault), s"expected an ownership fault, got $faults")
    }
  }

  test("a synchronous failure while submitting completes the effect instead of hanging") {
    val (scope, _) = txScope(p => if (p.asUtf8String.contains("HELLO")) Seq(helloReply) else throw ConnectionLost(mayHaveExecuted = false))
    scope.run(Strings.set("k", "v")).unsafeRun.failed.map(e => assert(e.isInstanceOf[ConnectionLost], s"expected ConnectionLost, got $e"))
  }

  test("a transaction whose EXEC hits a READONLY invokes the onFault hook") {
    val respond: Bytes => Seq[Frame] = p =>
      if (p.asUtf8String.contains("HELLO")) Seq(helloReply)
      else if (p.asUtf8String.contains("MULTI")) Seq(Frame.SimpleString("OK"), readonly, Frame.SimpleError("EXECABORT discarded"))
      else Seq(Frame.SimpleString("OK"))
    val (scope, faults)              = txScope(respond)
    scope.exec(Vector(Strings.set("k", "v"))).unsafeRun.failed.map { _ =>
      assert(faults.exists(isOwnershipFault), s"expected an ownership fault, got $faults")
    }
  }
}
