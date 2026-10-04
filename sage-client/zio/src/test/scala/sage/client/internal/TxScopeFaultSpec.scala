package sage.client.internal

import scala.collection.mutable
import scala.concurrent.ExecutionContext

import kyo.compat.*

import sage.Bytes
import sage.SageException.{ConnectionLost, TransactionDiscarded}
import sage.client.DedicatedPoolConfig
import sage.commands.{Connection, Strings}
import sage.protocol.Frame

class TxScopeFaultSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val readonly = Frame.SimpleError("READONLY You can't write against a read only replica.")

  private def txScope(respond: Bytes => Seq[Frame]): (Client.TxScope, mutable.ArrayBuffer[RefreshPolicy]) = {
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = ScriptedTransport.factory(respond)
    val pool                                            =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, () => true, DedicatedPoolConfig(), 1000L)
    val faults                                          = mutable.ArrayBuffer.empty[RefreshPolicy]
    (new Client.TxScope(pool.acquireForTransaction(), pool.releaseTransaction, faults += _), faults)
  }

  test("a READONLY command fault requests a forced refresh") {
    val (scope, faults) = txScope(p => if (p.asUtf8String.contains("HELLO")) Seq(Replies.hello) else Seq(readonly))
    scope.run(Strings.set("k", "v")).unsafeRun.failed.map { _ =>
      assert(faults.contains(RefreshPolicy.Forced), s"expected a forced refresh, got $faults")
    }
  }

  test("a synchronous failure while submitting completes the effect instead of hanging") {
    val (scope, _) = txScope(p => if (p.asUtf8String.contains("HELLO")) Seq(Replies.hello) else throw ConnectionLost(mayHaveExecuted = false))
    scope.run(Strings.set("k", "v")).unsafeRun.failed.map(e => assert(e.isInstanceOf[ConnectionLost], s"expected ConnectionLost, got $e"))
  }

  test("a transaction whose queued command hits a READONLY is discarded and requests a forced refresh") {
    val respond: Bytes => Seq[Frame] = p =>
      if (p.asUtf8String.contains("HELLO")) Seq(Replies.hello)
      else if (p.asUtf8String.contains("MULTI")) Seq(Frame.SimpleString("OK"), readonly, Frame.SimpleError("EXECABORT discarded"))
      else Seq(Frame.SimpleString("OK"))
    val (scope, faults)              = txScope(respond)
    scope.exec(Vector(Strings.set("k", "v"))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[TransactionDiscarded] && error.getMessage.contains("READONLY"), s"expected TransactionDiscarded, got $error")
      assert(faults.contains(RefreshPolicy.Forced), s"expected a forced refresh, got $faults")
    }
  }
}
