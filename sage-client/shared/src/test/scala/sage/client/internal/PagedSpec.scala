package sage.client.internal

import scala.concurrent.ExecutionContext

import kyo.compat.*

import sage.commands.{ScanCursor, ScanPage, StreamEntry, StreamId, StreamRangeId, XAutoClaimResult}

/**
  * Drives each [[Paged]] stepper directly with a scripted `CIO` fetch — the deterministic test surface the extraction unlocks. No server,
  * no backend stream type: just the cursor-termination, tombstone-skipping and pending-then-tail logic the four backends share.
  */
class PagedSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private def entry(ms: Long): StreamEntry[String, String]     = StreamEntry(StreamId(ms, 0L), Vector("f" -> "v"))
  private def tombstone(ms: Long): StreamEntry[String, String] = StreamEntry(StreamId(ms, 0L), Vector.empty)

  test("byCursor emits the page, and stops when the server returns no continuation cursor") {
    val more = Paged.byCursor[String](_ => CIO.value(ScanPage(Vector("a", "b"), Some(ScanCursor.start))))
    val done = Paged.byCursor[String](_ => CIO.value(ScanPage(Vector("c"), None)))
    for {
      m   <- more(Some(ScanCursor.start)).unsafeRun
      d   <- done(Some(ScanCursor.start)).unsafeRun
      end <- done(None).unsafeRun
    } yield {
      assertEquals(m.map(_._1), Some(Vector("a", "b")))
      assert(m.flatMap(_._2).isDefined, "a continuation cursor should keep the scan going")
      assertEquals(d.map(_._1), Some(Vector("c")))
      assert(d.get._2.isEmpty, "a zero cursor should end the scan")
      assertEquals(end, None)
    }
  }

  test("acrossTargets walks every target to its own zero cursor, then ends; empty targets end immediately") {
    val fetch: ScanTarget => ScanCursor => CIO[ScanPage[String]] = _ => _ => CIO.value(ScanPage(Vector("x"), None))
    val step                                                     = Paged.acrossTargets(CIO.value(Vector(ScanTarget.any, ScanTarget.any)))(fetch)
    val none                                                     = Paged.acrossTargets[String](CIO.value(Vector.empty[ScanTarget]))(fetch)
    for {
      begin  <- step(ScanStep.Begin).unsafeRun
      visit1 <- step(begin.get._2).unsafeRun
      visit2 <- step(visit1.get._2).unsafeRun
      end    <- step(ScanStep.End).unsafeRun
      empty  <- none(ScanStep.Begin).unsafeRun
    } yield {
      assertEquals(begin.get._1, Vector.empty[String])
      begin.get._2 match {
        case ScanStep.Visit(_, remaining) => assertEquals(remaining.length, 2)
        case other                        => fail(s"expected Visit over two targets, got $other")
      }
      assertEquals(visit1.get._1, Vector("x"))
      visit1.get._2 match {
        case ScanStep.Visit(_, remaining) => assertEquals(remaining.length, 1)
        case other                        => fail(s"expected Visit over the last target, got $other")
      }
      assertEquals(visit2.get._1, Vector("x"))
      assertEquals(visit2.get._2, ScanStep.End)
      assertEquals(end, None)
      assertEquals(empty, None)
    }
  }

  test("byRange advances past the last id while pages are full, and ends on a short or empty page") {
    val full  = Vector(entry(1), entry(2), entry(3))
    val step  = Paged.byRange[String, String](batch = 3) {
      case StreamRangeId.Min => CIO.value(full)
      case _                 => CIO.value(Vector(entry(4)))
    }
    val drain = Paged.byRange[String, String](batch = 3)(_ => CIO.value(Vector.empty[StreamEntry[String, String]]))
    for {
      page1 <- step(Some(StreamRangeId.Min)).unsafeRun
      page2 <- step(page1.get._2).unsafeRun
      end   <- step(None).unsafeRun
      empty <- drain(Some(StreamRangeId.Min)).unsafeRun
    } yield {
      assertEquals(page1, Some((full, Some(StreamRangeId.Exclusive(StreamId(3L, 0L))))))
      assertEquals(page2, Some((Vector(entry(4)), None)))
      assertEquals(end, None)
      assertEquals(empty, None)
    }
  }

  test("byAutoClaim skips tombstones and ends when the cursor wraps to Zero") {
    val step = Paged.byAutoClaim[String, String] {
      case StreamId.Zero => CIO.value(XAutoClaimResult(StreamId(5L, 0L), Vector(entry(1), tombstone(2), entry(3)), Vector.empty))
      case _             => CIO.value(XAutoClaimResult(StreamId.Zero, Vector(entry(4)), Vector.empty))
    }
    for {
      first <- step(Some(StreamId.Zero)).unsafeRun
      last  <- step(first.get._2).unsafeRun
      end   <- step(None).unsafeRun
    } yield {
      assertEquals(first, Some((Vector(entry(1), entry(3)), Some(StreamId(5L, 0L)))))
      assertEquals(last, Some((Vector(entry(4)), None)))
      assertEquals(end, None)
    }
  }

  test("tail advances past the last id and holds position on an empty round") {
    val step = Paged.tail[String, String] {
      case StreamId.Zero => CIO.value(Vector(entry(1)))
      case _             => CIO.value(Vector.empty[StreamEntry[String, String]])
    }
    for {
      first <- step(StreamId.Zero).unsafeRun
      empty <- step(first.get._2).unsafeRun
    } yield {
      assertEquals(first, Some((Vector(entry(1)), StreamId(1L, 0L))))
      assertEquals(empty, Some((Vector.empty[StreamEntry[String, String]], StreamId(1L, 0L))))
    }
  }

  test("consume drains this consumer's pending history (Left), then switches to tailing new entries (Right)") {
    val step = Paged.consume[String, String](
      drainPending = {
        case StreamId.Zero => CIO.value(Vector(entry(1)))
        case _             => CIO.value(Vector.empty[StreamEntry[String, String]])
      },
      tailNew = CIO.value(Vector(entry(2)))
    )
    for {
      pending1 <- step(Left(StreamId.Zero)).unsafeRun
      pending2 <- step(pending1.get._2).unsafeRun
      tailing  <- step(pending2.get._2).unsafeRun
      tailing2 <- step(tailing.get._2).unsafeRun
    } yield {
      assertEquals(pending1, Some((Vector(entry(1)), Left(StreamId(1L, 0L)))))
      assertEquals(pending2, Some((Vector.empty[StreamEntry[String, String]], Right(()))))
      assertEquals(tailing, Some((Vector(entry(2)), Right(()))))
      // a second tail round re-issues XREADGROUP NEW: tailNew is a re-runnable CIO description, not consumed once
      assertEquals(tailing2, Some((Vector(entry(2)), Right(()))))
    }
  }
}
