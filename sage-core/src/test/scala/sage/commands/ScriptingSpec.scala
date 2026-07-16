package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

class ScriptingSpec extends munit.FunSuite {

  private def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  test("EVAL returns the raw RESP3 frame untouched") {
    assertEquals(Reply.run(Scripting.eval("return 1"), Frame.Integer(1L)), Right(Frame.Integer(1L)))
    val nested = Frame.Array(Vector(bulk("a"), Frame.Integer(2L)))
    assertEquals(Reply.run(Scripting.eval("x", Seq("k")), nested), Right(nested))
  }

  test("EVAL computes numkeys and key indices from the key list") {
    val command = Scripting.eval("s", Seq("k1", "k2"), Seq("a"))
    assertEquals(command.args.map(_.asUtf8String), Vector("s", "2", "k1", "k2", "a"))
    assertEquals(command.keyIndices, Vector(2, 3))
    assertEquals(command.keys.map(_.asUtf8String), Vector("k1", "k2"))
  }

  test("a zero-key EVAL is keyless") {
    assertEquals(Scripting.eval("return 1").keyIndices, Vector.empty)
  }

  test("the _RO variants are read-only but never cacheable") {
    assert(Scripting.evalRo("s").isReadOnly)
    assert(!Scripting.evalRo("s").cacheable)
    assert(Scripting.evalShaRo("sha").isReadOnly)
  }

  test("SCRIPT EXISTS decodes one flag per sha, in order") {
    val reply = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(0L), Frame.Integer(1L)))
    assertEquals(Reply.run(Scripting.scriptExists("a", "b", "c"), reply), Right(Vector(true, false, true)))
  }

  test("SCRIPT LOAD decodes the sha") {
    assertEquals(Reply.run(Scripting.scriptLoad("return 1"), bulk("abc123")), Right("abc123"))
  }

  test("SCRIPT LOAD, FLUSH and EXISTS are All-Masters Commands; KILL and EVALSHA are not") {
    assert(Scripting.scriptLoad("s").allMasters)
    assert(Scripting.scriptFlush().allMasters)
    assert(Scripting.scriptExists("a").allMasters)
    assert(!Scripting.scriptKill.allMasters)
    assert(!Scripting.evalSha("sha").allMasters)
  }

  test("SCRIPT EXISTS folds per-sha presence across masters with AND") {
    val BroadcastReduce.Fold(combine) = Scripting.scriptExists("a", "b").broadcast: @unchecked
    val a                             = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(1L)))
    val b                             = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(0L)))
    assertEquals(combine(a, b), Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(0L))))
  }

  test("SCRIPT EXISTS surfaces a non-binary flag so the strict decode rejects it rather than coercing to false") {
    val BroadcastReduce.Fold(combine) = Scripting.scriptExists("a").broadcast: @unchecked
    val merged                        = combine(Frame.Array(Vector(Frame.Integer(2L))), Frame.Array(Vector(Frame.Integer(1L))))
    assert(Reply.run(Scripting.scriptExists("a"), merged).isLeft)
  }

  test("SCRIPT EXISTS fails a length mismatch across masters rather than hiding the malformed reply") {
    val BroadcastReduce.Fold(combine) = Scripting.scriptExists("a", "b").broadcast: @unchecked
    val whole                         = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(1L)))
    val short                         = Frame.Array(Vector(Frame.Integer(1L)))
    intercept[DecodeError](combine(whole, short))
  }
}
