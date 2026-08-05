package sage.client.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.BlockTimeout
import sage.commands.{ScanCursor, ScanPage, StreamEntry, StreamId, StreamRangeId, XAutoClaimResult}

/**
  * Shared paging logic for streaming helpers such as `scanAll`, `xRangeAll`, and `xConsume`. Each builder accepts a `CIO` function that
  * fetches one page and returns a step shaped as `S => CIO[Option[(page, nextState)]]`. ZIO, Cats Effect, and Ox use the step with
  * `CStream.unfold`. Kyo uses its native `Stream.unfold` with `chunkSize = 1`, which lets unbounded streams emit each page immediately. The
  * shared code handles cursor completion, deleted entries, and consumer recovery; each backend only constructs its stream and lifts the
  * page fetch. `PagedSpec` tests the steps directly with scripted fetches.
  */
private[sage] object Paged {

  /**
    * A page step: from state `S`, fetch the next page of `A`s and the state to resume from, or `None` to end the stream.
    */
  type Step[S, A] = S => CIO[Option[(Vector[A], S)]]

  // a finite poll lets xTail and xConsume check for cancellation between blocking reads.
  val defaultPoll: BlockTimeout = BlockTimeout.After(FiniteDuration(5, TimeUnit.SECONDS))

  /**
    * Pages through HSCAN, SSCAN, ZSCAN, or one SCAN target until the server returns a zero cursor. A filtered scan can return an empty page
    * with a non-zero cursor, so an empty page does not end iteration.
    */
  def byCursor[A](fetch: ScanCursor => CIO[ScanPage[A]]): Step[Option[ScanCursor], A] = {
    case None         => CIO.value(None)
    case Some(cursor) => fetch(cursor).map(page => Some((page.items, page.next)))
  }

  /**
    * Scans every cluster target in turn, completing one node-local cursor before moving to the next. `Begin` discovers the targets, and an
    * empty target list ends the stream immediately.
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
    * Pages through XAUTOCLAIM until its cursor returns to `StreamId.Zero`. Entries whose data has already been deleted are omitted.
    */
  def byAutoClaim[F, V](fetch: StreamId => CIO[XAutoClaimResult[F, V]]): Step[Option[StreamId], StreamEntry[F, V]] = {
    case None       => CIO.value(None)
    case Some(from) =>
      fetch(from).map(result => Some((result.entries.filter(_.fields.nonEmpty), if (result.cursor == StreamId.Zero) None else Some(result.cursor))))
  }

  /**
    * Replays every XREAD entry after `from`, then waits for new entries. After a non-empty read, the next read starts after the last returned
    * id. An empty read keeps the same id.
    */
  def tail[F, V](fetch: StreamId => CIO[Vector[StreamEntry[F, V]]]): Step[StreamId, StreamEntry[F, V]] =
    last => fetch(last).map(entries => Some((entries, if (entries.isEmpty) last else entries.last.id)))

  /**
    * Reads this consumer's pending XREADGROUP entries first, which supports at-least-once recovery after a restart. It then waits for new
    * entries. `Left` stores the pending-entry cursor and `Right` marks the switch to new entries. Each backend acknowledges an entry only
    * after the user's handler succeeds. If the handler fails, the entry remains pending for later recovery.
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
