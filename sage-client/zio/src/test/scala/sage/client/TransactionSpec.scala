package sage.client

import scala.concurrent.ExecutionContext

import kyo.compat.*

import sage.Bytes
import sage.SageException.{InvalidArgument, ServerError, TransactionDiscarded}
import sage.client.internal.{Client, FakeTransport, MultiplexedConnection}
import sage.commands.{Command, Commands, Execution}
import sage.protocol.Frame

class TransactionSpec extends munit.FunSuite {

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

  private val ok: Frame     = Frame.SimpleString("OK")
  private val queued: Frame = Frame.SimpleString("QUEUED")

  // Scripts a dedicated connection: HELLO bootstraps, WATCH/UNWATCH answer OK, the pipelined MULTI…EXEC batch (one write) answers with the
  // supplied frame sequence, and a read GET answers a value. The batch carries "MULTI", so it is matched before the bare command names.
  private def scripted(execReplies: Seq[Frame], getReply: Frame = Frame.BulkString(Bytes.utf8("v"))): MultiplexedConnection.TransportFactory =
    (onFrame, onClosed) =>
      new FakeTransport(
        onFrame,
        onClosed,
        respond = payload => {
          val text = payload.asUtf8String
          if (text.contains("HELLO")) Seq(helloReply)
          else if (text.contains("MULTI")) execReplies
          else if (text.contains("UNWATCH")) Seq(ok)
          else if (text.contains("WATCH")) Seq(ok)
          else if (text.contains("GET")) Seq(getReply)
          else Seq(ok)
        }
      )

  // wraps a factory so the most-recently-created transport (the dedicated connection) can be inspected after the run
  private def capturing(factory: MultiplexedConnection.TransportFactory): (MultiplexedConnection.TransportFactory, () => FakeTransport) = {
    var last: FakeTransport                             = null
    val wrapped: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      last = factory(onFrame, onClosed).asInstanceOf[FakeTransport]
      last
    }
    (wrapped, () => last)
  }

  private val twoIncrements = (Commands.incr[String]("a"), Commands.incr[String]("b"))

  private val noCommands = Vector.empty[Command[Long]]

  test("a transaction commits and returns the typed tuple") {
    val factory = scripted(Seq(ok, queued, queued, Frame.Array(Vector(Frame.Integer(1), Frame.Integer(2)))))
    Client.connectWith(factory).flatMap(_.transaction(_.exec(twoIncrements))).unsafeRun.map { result =>
      assertEquals(result, Some((1L, 2L)))
    }
  }

  test("a watched key change aborts the transaction with None") {
    val factory = scripted(Seq(ok, queued, queued, Frame.Null))
    Client
      .connectWith(factory)
      .flatMap(client => client.transaction(tx => tx.watch("a").flatMap(_ => tx.exec(twoIncrements))))
      .unsafeRun
      .map(result => assertEquals(result, None))
  }

  test("a queueing-phase error discards the whole transaction") {
    val factory = scripted(
      Seq(ok, Frame.SimpleError("ERR unknown command 'FOO'"), Frame.SimpleError("EXECABORT Transaction discarded because of previous errors"))
    )
    val single  = Vector(Commands.incr[String]("a"))
    Client.connectWith(factory).flatMap(_.transaction(_.exec(single))).unsafeRun.failed.map { error =>
      assertEquals(error, TransactionDiscarded("ERR unknown command 'FOO'"))
    }
  }

  test("an execution-phase error fails strict exec with the first error") {
    val factory = scripted(Seq(ok, queued, queued, Frame.Array(Vector(Frame.Integer(1), Frame.SimpleError("WRONGTYPE nope")))))
    Client.connectWith(factory).flatMap(_.transaction(_.exec(twoIncrements))).unsafeRun.failed.map { error =>
      assertEquals(error, ServerError("WRONGTYPE", "nope"))
    }
  }

  test("execAttempt surfaces execution-phase errors per-position while other commands commit") {
    val factory = scripted(Seq(ok, queued, queued, Frame.Array(Vector(Frame.Integer(1), Frame.SimpleError("WRONGTYPE nope")))))
    Client.connectWith(factory).flatMap(_.transaction(_.execAttempt(twoIncrements))).unsafeRun.map { result =>
      val (a, b) = result.getOrElse(fail("expected a committed transaction"))
      assertEquals(a, Right(1L))
      assertEquals(b, Left(ServerError("WRONGTYPE", "nope")))
    }
  }

  test("discard issues UNWATCH and runs no EXEC") {
    val (factory, transport) = capturing(scripted(Seq(ok)))
    Client
      .connectWith(factory)
      .flatMap(client => client.transaction(tx => tx.watch("a").flatMap(_ => tx.discard)))
      .unsafeRun
      .map { _ =>
        assert(transport().written.exists(_.asUtf8String.contains("UNWATCH")), "expected an UNWATCH")
        assert(!transport().written.exists(_.asUtf8String.contains("EXEC")), "expected no EXEC")
      }
  }

  test("a scope captured beyond the block rejects further use") {
    val factory = scripted(Seq(ok))
    Client
      .connectWith(factory)
      .flatMap(client => client.transaction(tx => CIO.value(tx)).flatMap(escaped => escaped.run(Commands.incr[String]("a"))))
      .unsafeRun
      .failed
      .map(error => assert(error.isInstanceOf[IllegalStateException], s"unexpected error: $error"))
  }

  test("a captured scope rejects exec even of an empty pipeline after the block") {
    val factory = scripted(Seq(ok))
    Client
      .connectWith(factory)
      .flatMap(client => client.transaction(tx => CIO.value(tx)).flatMap(escaped => escaped.exec(noCommands)))
      .unsafeRun
      .failed
      .map(error => assert(error.isInstanceOf[IllegalStateException], s"unexpected error: $error"))
  }

  test("an empty transaction with watches still issues MULTI/EXEC so a concurrent change can abort it") {
    val (factory, transport) = capturing(scripted(Seq(ok, Frame.Null))) // MULTI ok, EXEC null = watched key changed
    Client
      .connectWith(factory)
      .flatMap(client => client.transaction(tx => tx.watch("a").flatMap(_ => tx.exec(noCommands))))
      .unsafeRun
      .map { result =>
        assertEquals(result, None)
        assert(transport().written.exists(_.asUtf8String.contains("MULTI")), "expected MULTI/EXEC for an empty watched transaction")
      }
  }

  test("an empty transaction with no watch is a no-op that never reaches the socket") {
    val (factory, transport) = capturing(scripted(Seq(ok)))
    Client
      .connectWith(factory)
      .flatMap(_.transaction(_.exec(noCommands)))
      .unsafeRun
      .map { result =>
        assertEquals(result, Some(Vector.empty[Long]))
        assert(!transport().written.exists(_.asUtf8String.contains("MULTI")), "an empty no-watch transaction must not touch the socket")
      }
  }

  test("a transaction carrying a blocking command fails fast without reaching the socket") {
    val factory  = scripted(Seq(ok))
    val blocking = Vector(Command("BLPOP", Command.NoKeys, Vector.empty, _ => Right(()), Execution.Blocking))
    Client.connectWith(factory).flatMap(_.transaction(_.exec(blocking))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[InvalidArgument], s"unexpected error: $error")
    }
  }

  test("running a blocking command in the read phase fails fast rather than hanging the leased connection") {
    val (factory, transport) = capturing(scripted(Seq(ok)))
    val blocking             = Command("BLPOP", Command.NoKeys, Vector.empty, _ => Right(()), Execution.Blocking)
    Client.connectWith(factory).flatMap(_.transaction(_.run(blocking))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[InvalidArgument], s"unexpected error: $error")
      assert(!transport().written.exists(_.asUtf8String.contains("BLPOP")), "the blocking command must not reach the socket")
    }
  }
}
