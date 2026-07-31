package sage.commands

import sage.Bytes
import sage.SageException.{DecodeError, ServerError}
import sage.protocol.{Frame, RespParser}
import sage.protocol.Frames.bulk

class CommandSpec extends munit.FunSuite {

  test("cacheable marks deterministic key-state reads, not writes, time-varying, or non-deterministic reads") {
    assert(Strings.get[String, String]("foo").cacheable)
    assert(Hashes.hGet[String, String, String]("h", "f").cacheable)
    assert(!Strings.set("foo", "bar").cacheable)                                                                 // a write is never cacheable
    assert(!Keys.ttl("foo").cacheable && Keys.ttl("foo").isReadOnly)                                             // read-only but time-varying
    assert(!Sets.sRandMember[String, String]("s").cacheable && Sets.sRandMember[String, String]("s").isReadOnly) // non-deterministic
  }

  test("SCAN-family commands are cursor-bound (node-local cursor) and not cacheable, so replica routing never round-robins them") {
    val scans = Vector(
      Keys.scan[String](ScanCursor.start),
      Hashes.hScan[String, String, String]("h", ScanCursor.start),
      Hashes.hScanNoValues[String, String]("h", ScanCursor.start),
      Sets.sScan[String, String]("s", ScanCursor.start),
      SortedSets.zScan[String, String]("z", ScanCursor.start)
    )
    scans.foreach(c => assert(c.cursorBound && c.isReadOnly && !c.cacheable, s"${c.name} should be a cursor-bound, non-cacheable read"))
    assert(!Strings.get[String, String]("foo").cursorBound) // an ordinary read is not cursor-bound
  }

  test("SORT_RO is cacheable only in its bare form; BY/GET dereference untracked keys") {
    assert(Keys.sortRo[String, String]("k").cacheable)
    assert(Keys.sortRo[String, String]("k", alpha = true, order = SortOrder.Desc).cacheable) // limit/order/alpha touch no extra keys
    assert(!Keys.sortRo[String, String]("k", by = Some("w_*")).cacheable && Keys.sortRo[String, String]("k", by = Some("w_*")).isReadOnly)
    assert(!Keys.sortRo[String, String]("k", get = Vector("d_*")).cacheable && Keys.sortRo[String, String]("k", get = Vector("d_*")).isReadOnly)
  }

  test("multi-word command names encode one bulk string per word") {
    val command = Command[Unit]("CONFIG GET", Vector.empty, Vector(Bytes.utf8("maxmemory")), _ => Right(()))
    assertEquals(command.encode.asUtf8String, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n")
  }

  test("a command's encoded bytes parse back as an array of bulk strings") {
    val parser   = new RespParser
    val expected = Frame.Array(Vector(bulk("SET"), bulk("key"), bulk("value")))
    assertEquals(parser.feed(Strings.set("key", "value").encode), Right(Vector(expected)))
  }

  test("keyIndices marks the key positions in args for the slot engine") {
    val command = Strings.get[String, String]("foo")
    assertEquals(command.keyIndices, Vector(0))
    assert(command.args(command.keyIndices.head).sameBytes(Bytes.utf8("foo")))
    assertEquals(Strings.set("foo", "bar").keyIndices, Vector(0))
    assertEquals(Connection.ping().keyIndices, Vector.empty[Int])
  }

  test("a top-level error frame becomes a ServerError for any command") {
    assertEquals(Reply.run(Strings.get[String, String]("foo"), Frame.SimpleError("ERR oops")), Left(ServerError("ERR", "oops")))
    assertEquals(Reply.run(Strings.set("foo", "bar"), Frame.BulkError(Bytes.utf8("WRONGTYPE bad"))), Left(ServerError("WRONGTYPE", "bad")))
  }

  test("an unexpected frame shape becomes a DecodeError naming expected and actual") {
    Reply.run(Strings.mSet(("foo", "bar")), Frame.Integer(1)) match {
      case Left(error: DecodeError) =>
        assertEquals(error.expected, "simple string 'OK'")
        assertEquals(error.actual, "integer 1")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("map transforms the decoded result") {
    val exists = Strings.get[String, String]("foo").map(_.isDefined)
    assertEquals(Reply.run(exists, bulk("bar")), Right(true))
    assertEquals(Reply.run(exists, Frame.Null), Right(false))
  }
}
