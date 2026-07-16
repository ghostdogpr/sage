package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

class ScriptingSpec extends munit.FunSuite {

  private def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  private def flags(bits: Long*): Frame = Frame.Array(bits.iterator.map(Frame.Integer(_)).toVector)

  private val existsFold: (Frame, Frame) => Frame = {
    val BroadcastReduce.Fold(combine) = Scripting.scriptExists("x").broadcast: @unchecked
    combine
  }

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
    assertEquals(existsFold(flags(1L, 1L), flags(1L, 0L)), flags(1L, 0L))
  }

  test("SCRIPT EXISTS surfaces a non-binary flag so the strict decode rejects it rather than coercing to false") {
    assert(Reply.run(Scripting.scriptExists("a"), existsFold(flags(2L), flags(1L))).isLeft)
  }

  test("SCRIPT EXISTS fails a length mismatch across masters rather than hiding the malformed reply") {
    intercept[DecodeError](existsFold(flags(1L, 1L), flags(1L)))
  }
}
