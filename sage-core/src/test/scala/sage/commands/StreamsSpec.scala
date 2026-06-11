package sage.commands

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.protocol.Frame

class StreamsSpec extends munit.FunSuite {

  private def bulk(value: String): Frame                = Frame.BulkString(Bytes.utf8(value))
  private def entry(id: String, fields: String*): Frame = Frame.Array(Vector(bulk(id), Frame.Array(fields.toVector.map(bulk))))

  test("XADD decodes the generated id; NOMKSTREAM decodes null as None") {
    assertEquals(Reply.run(Streams.xAdd("k")(("f", "v")), bulk("1526919030474-55")), Right(StreamId(1526919030474L, 55L)))
    assertEquals(Reply.run(Streams.xAddNoMkStream("k")(("f", "v")), Frame.Null), Right(None))
    assertEquals(Reply.run(Streams.xAddNoMkStream("k")(("f", "v")), bulk("5-0")), Right(Some(StreamId(5L, 0L))))
  }

  test("XRANGE decodes entries, preserving field order and a missing-stream null as empty? (present array)") {
    val reply = Frame.Array(Vector(entry("1-0", "a", "1", "b", "2"), entry("2-0", "c", "3")))
    assertEquals(
      Reply.run(Streams.xRange[String, String, String]("k"), reply),
      Right(Vector(StreamEntry(StreamId(1L, 0L), Vector("a" -> "1", "b" -> "2")), StreamEntry(StreamId(2L, 0L), Vector("c" -> "3"))))
    )
  }

  test("XREAD decodes a RESP3 map of stream -> entries, and null/timeout as an empty vector") {
    val reply = Frame.Map(Vector(bulk("s1") -> Frame.Array(Vector(entry("1-0", "f", "v")))))
    assertEquals(
      Reply.run(Streams.xRead[String, String, String](("s1", ReadId.New))(), reply),
      Right(Vector("s1" -> Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v")))))
    )
    assertEquals(Reply.run(Streams.xRead[String, String, String](("s1", ReadId.New))(), Frame.Null), Right(Vector.empty))
  }

  test("XREAD also accepts the RESP2 array-of-pairs shape") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(bulk("s1"), Frame.Array(Vector(entry("1-0", "f", "v")))))))
    assertEquals(
      Reply.run(Streams.xRead[String, String, String](("s1", ReadId.New))(), reply),
      Right(Vector("s1" -> Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v")))))
    )
  }

  test("XAUTOCLAIM decodes the cursor/entries/deleted triple and the pre-7.0 two-element form") {
    val three = Frame.Array(Vector(bulk("5-0"), Frame.Array(Vector(entry("1-0", "f", "v"))), Frame.Array(Vector(bulk("2-0")))))
    assertEquals(
      Reply.run(Streams.xAutoClaim[String, String, String]("k", "g", "c", FiniteDuration(1, TimeUnit.SECONDS)), three),
      Right(XAutoClaimResult(StreamId(5L, 0L), Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v"))), Vector(StreamId(2L, 0L))))
    )
    val two   = Frame.Array(Vector(bulk("0-0"), Frame.Array(Vector(entry("1-0", "f", "v")))))
    assertEquals(
      Reply.run(Streams.xAutoClaim[String, String, String]("k", "g", "c", FiniteDuration(1, TimeUnit.SECONDS)), two),
      Right(XAutoClaimResult(StreamId(0L, 0L), Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v"))), Vector.empty))
    )
  }

  test("XAUTOCLAIM tolerates a tombstone entry [id, nil] as an entry with no fields") {
    val reply = Frame.Array(Vector(bulk("0-0"), Frame.Array(Vector(Frame.Array(Vector(bulk("1-0"), Frame.Null)))), Frame.Array(Vector.empty)))
    assertEquals(
      Reply.run(Streams.xAutoClaim[String, String, String]("k", "g", "c", FiniteDuration(1, TimeUnit.SECONDS)), reply),
      Right(XAutoClaimResult(StreamId(0L, 0L), Vector(StreamEntry(StreamId(1L, 0L), Vector.empty)), Vector.empty))
    )
  }

  test("XPENDING summary decodes the populated form and the empty group") {
    val populated = Frame.Array(
      Vector(
        Frame.Integer(2L),
        bulk("1-0"),
        bulk("9-0"),
        Frame.Array(Vector(Frame.Array(Vector(bulk("c1"), bulk("1"))), Frame.Array(Vector(bulk("c2"), bulk("1")))))
      )
    )
    assertEquals(
      Reply.run(Streams.xPending("k", "g"), populated),
      Right(PendingSummary(2L, Some(StreamId(1L, 0L)), Some(StreamId(9L, 0L)), Vector("c1" -> 1L, "c2" -> 1L)))
    )
    val empty     = Frame.Array(Vector(Frame.Integer(0L), Frame.Null, Frame.Null, Frame.Null))
    assertEquals(Reply.run(Streams.xPending("k", "g"), empty), Right(PendingSummary(0L, None, None, Vector.empty)))
  }

  test("XPENDING extended decodes a row with its idle time and delivery count") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(bulk("1-0"), bulk("c1"), Frame.Integer(5000L), Frame.Integer(3L)))))
    assertEquals(
      Reply.run(Streams.xPendingExtended("k", "g"), reply),
      Right(Vector(PendingEntry(StreamId(1L, 0L), "c1", FiniteDuration(5000L, TimeUnit.MILLISECONDS), 3L)))
    )
  }

  test("XDELEX decodes the per-id deletion status") {
    val reply = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(-1L), Frame.Integer(2L)))
    assertEquals(
      Reply.run(Streams.xDelEx("k")(StreamId(1L, 0L), StreamId(2L, 0L), StreamId(3L, 0L)), reply),
      Right(Vector(StreamEntryDeletion.Deleted, StreamEntryDeletion.NotFound, StreamEntryDeletion.Retained))
    )
  }

  test("XINFO STREAM decodes leniently: 7.0 fields present, and absent as None") {
    val withNew = Frame.Map(
      Vector(
        bulk("length")                  -> Frame.Integer(2L),
        bulk("radix-tree-keys")         -> Frame.Integer(1L),
        bulk("radix-tree-nodes")        -> Frame.Integer(2L),
        bulk("last-generated-id")       -> bulk("5-0"),
        bulk("max-deleted-entry-id")    -> bulk("3-0"),
        bulk("entries-added")           -> Frame.Integer(7L),
        bulk("recorded-first-entry-id") -> bulk("1-0"),
        bulk("groups")                  -> Frame.Integer(1L),
        bulk("first-entry")             -> entry("1-0", "f", "v"),
        bulk("last-entry")              -> entry("5-0", "g", "w")
      )
    )
    val info    = Reply.run(StreamInfo.xInfoStream[String, String, String]("k"), withNew).toOption.get
    assertEquals(info.length, 2L)
    assertEquals(info.entriesAdded, Some(7L))
    assertEquals(info.maxDeletedEntryId, Some(StreamId(3L, 0L)))
    assertEquals(info.firstEntry, Some(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v"))))

    val legacy = Frame.Map(
      Vector(
        bulk("length")            -> Frame.Integer(0L),
        bulk("radix-tree-keys")   -> Frame.Integer(0L),
        bulk("radix-tree-nodes")  -> Frame.Integer(1L),
        bulk("last-generated-id") -> bulk("0-0"),
        bulk("groups")            -> Frame.Integer(0L),
        bulk("first-entry")       -> Frame.Null,
        bulk("last-entry")        -> Frame.Null
      )
    )
    val old    = Reply.run(StreamInfo.xInfoStream[String, String, String]("k"), legacy).toOption.get
    assertEquals(old.entriesAdded, None)
    assertEquals(old.maxDeletedEntryId, None)
    assertEquals(old.firstEntry, None)
  }

  test("XINFO GROUPS decodes entries-read/lag as optional, tolerating a null lag") {
    val reply = Frame.Array(
      Vector(
        Frame.Map(
          Vector(
            bulk("name")              -> bulk("g1"),
            bulk("consumers")         -> Frame.Integer(2L),
            bulk("pending")           -> Frame.Integer(3L),
            bulk("last-delivered-id") -> bulk("5-0"),
            bulk("entries-read")      -> Frame.Integer(5L),
            bulk("lag")               -> Frame.Null
          )
        )
      )
    )
    assertEquals(
      Reply.run(StreamInfo.xInfoGroups("k"), reply),
      Right(Vector(GroupInfo("g1", 2L, 3L, StreamId(5L, 0L), Some(5L), None)))
    )
  }
}
