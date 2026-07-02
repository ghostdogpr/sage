package sage.client.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.BlockTimeout
import sage.commands.{ScanCursor, ScanPage, StreamEntry, StreamId, StreamRangeId, XAutoClaimResult}

/**
  * The page-stepping state machines behind the per-backend `…All` streaming helpers (`scanAll`, `xRangeAll`, `xConsume`, …). Each builder
  * takes a `fetch` that pulls one page in the `CIO` carrier and returns a step `S => CIO[Option[(page, nextState)]]` — the shape both
  * `CStream.unfold` (zio/ce/ox) and the native `Stream.unfold` (kyo, which needs `chunkSize = 1` for its infinite tails) consume. The
  * cursor-termination, tombstone-skipping and pending-then-tail logic lives here once; the backend keeps only its own stream construction and
  * the `s => CIO.lift(client.run(…))` fetch. Pure given its `fetch`, so each step is driven directly in `PagedSpec` with a scripted fetch.
  */
private[sage] object Paged {

  /**
    * A page step: from state `S`, fetch the next page of `A`s and the state to resume from, or `None` to end the stream.
    */
  type Step[S, A] = S => CIO[Option[(Vector[A], S)]]

  // the tailing streams' default poll (xTail/xConsume); bounded so the blocking read returns periodically and cancellation stays responsive
  val defaultPoll: BlockTimeout = BlockTimeout.After(FiniteDuration(5, TimeUnit.SECONDS))

  /**
    * Cursor scan (HSCAN/SSCAN/ZSCAN, and a single SCAN target): page until the server returns no continuation cursor. Termination is on the
    * zero cursor only, never on an empty page — a `MATCH`-filtered scan routinely returns empty intermediate pages with a non-zero cursor, so
    * an `isEmpty => None` guard (as [[byRange]] and [[byAutoClaim]] carry) would end the sweep early and drop the rest of the keyspace.
    */
  def byCursor[A](fetch: ScanCursor => CIO[ScanPage[A]]): Step[Option[ScanCursor], A] = {
    case None         => CIO.value(None)
    case Some(cursor) => fetch(cursor).map(page => Some((page.items, page.next)))
  }

  /**
    * Cluster-wide SCAN: walk every target in turn, scanning each to its node-local zero cursor before moving on, so the sweep never
    * silently covers a single node. The `Begin` step discovers the targets; an empty set ends the stream immediately.
    */
  def acrossTargets[A](scanTargets: CIO[Vector[ScanTarget]])(fetch: ScanTarget => ScanCursor => CIO[ScanPage[A]]): Step[ScanStep, A] = {
    case ScanStep.Begin                    =>
      scanTargets.map(targets => if (targets.isEmpty) None else Some((Vector.empty[A], ScanStep.Visit(ScanCursor.start, targets))))
    case ScanStep.Visit(cursor, remaining) =>
      fetch(remaining.head)(cursor).map { page =>
        page.next match {
          case Some(next) => Some((page.items, ScanStep.Visit(next, remaining)))
          case None       => Some((page.items, if (remaining.tail.isEmpty) ScanStep.End else ScanStep.Visit(ScanCursor.start, remaining.tail)))
        }
      }
    case ScanStep.End                      => CIO.value(None)
  }

  /**
    * XRANGE paging: advance past the last id each page; a short page (fewer than `batch`) or an empty page ends the stream.
    */
  def byRange[F, V](batch: Long)(fetch: StreamRangeId => CIO[Vector[StreamEntry[F, V]]]): Step[Option[StreamRangeId], StreamEntry[F, V]] = {
    case None       => CIO.value(None)
    case Some(from) =>
      fetch(from).map { entries =>
        if (entries.isEmpty) None
        else Some((entries, if (entries.length < batch) None else Some(StreamRangeId.Exclusive(entries.last.id))))
      }
  }

  /**
    * XAUTOCLAIM paging: advance the cursor each call until it wraps back to the start (`StreamId.Zero`). Tombstone entries — claimed ids
    * whose data was already deleted, surfaced with no fields — are dropped so every emitted entry carries data.
    */
  def byAutoClaim[F, V](fetch: StreamId => CIO[XAutoClaimResult[F, V]]): Step[Option[StreamId], StreamEntry[F, V]] = {
    case None       => CIO.value(None)
    case Some(from) =>
      fetch(from).map(result => Some((result.entries.filter(_.fields.nonEmpty), if (result.cursor == StreamId.Zero) None else Some(result.cursor))))
  }

  /**
    * XREAD tail: replay every entry after `from`, then block for new ones forever, advancing past the last id and never advancing on an empty round.
    */
  def tail[F, V](fetch: StreamId => CIO[Vector[StreamEntry[F, V]]]): Step[StreamId, StreamEntry[F, V]] =
    last => fetch(last).map(entries => Some((entries, if (entries.isEmpty) last else entries.last.id)))

  /**
    * XREADGROUP consume: first drain this consumer's own pending history (`Left`, at-least-once recovery after a restart), then block for
    * new entries forever (`Right`). The per-entry acknowledgement is the backend's, layered over the entries this step yields.
    */
  def consume[F, V](
    drainPending: StreamId => CIO[Vector[StreamEntry[F, V]]],
    tailNew: CIO[Vector[StreamEntry[F, V]]]
  ): Step[Either[StreamId, Unit], StreamEntry[F, V]] = {
    case Left(after) =>
      drainPending(after).map(entries => if (entries.isEmpty) Some((Vector.empty, Right(()))) else Some((entries, Left(entries.last.id))))
    case Right(_)    =>
      tailNew.map(entries => Some((entries, Right(()))))
  }
}
