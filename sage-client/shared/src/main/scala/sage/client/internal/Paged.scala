package sage.client.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.BlockTimeout
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

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

  final case class Pages[S, A](init: S, step: Step[S, A])

  // scan each target in sequence with its own node-local cursor. A cluster scan visits every master that owns slots.
  def scanAll[K: KeyCodec](r: SharedRunner, pattern: Option[String], count: Option[Long], ofType: Option[RedisType]): Pages[ScanStep, K] =
    Pages(ScanStep.Begin, acrossTargets(r.scanTargets)(target => cursor => target.run(Keys.scan[K](cursor, pattern, count, ofType))))

  // HSCAN, SSCAN, or ZSCAN of one key
  def scanKey[A](r: SharedRunner)(page: ScanCursor => Command[ScanPage[A]]): Pages[Option[ScanCursor], A] =
    Pages(Some(ScanCursor.start), byCursor(cursor => r.run(page(cursor))))

  def xRangeAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    r: SharedRunner,
    key: K,
    start: StreamRangeId,
    end: StreamRangeId,
    batch: Long
  ): Pages[Option[StreamRangeId], StreamEntry[F, V]] =
    Pages(Some(start), byRange(batch)(from => r.run(Streams.xRange[K, F, V](key, from, end, Some(batch)))))

  def xAutoClaimAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    r: SharedRunner,
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId,
    count: Option[Long]
  ): Pages[Option[StreamId], StreamEntry[F, V]] =
    Pages(Some(start), byAutoClaim(from => r.run(Streams.xAutoClaim[K, F, V](key, group, consumer, minIdle, from, count))))

  def xTail[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    r: SharedRunner,
    key: K,
    from: StreamId,
    count: Option[Long],
    block: BlockTimeout
  ): Pages[StreamId, StreamEntry[F, V]] =
    Pages(from, tail(last => r.run(Streams.xRead[K, F, V]((key, ReadId.After(last)))(count = count, block = Some(block))).map(_.flatMap(_._2))))

  def xConsume[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    r: SharedRunner,
    group: String,
    consumer: String,
    key: K,
    count: Option[Long],
    block: BlockTimeout
  ): Pages[Either[StreamId, Unit], StreamEntry[F, V]] = {
    def read(id: GroupReadId, block: Option[BlockTimeout]): CIO[Vector[StreamEntry[F, V]]] =
      r.run(Streams.xReadGroup[K, F, V](group, consumer)((key, id))(count = count, block = block)).map(_.flatMap(_._2))
    Pages(Left(StreamId.Zero), consume(after => read(GroupReadId.After(after), None), read(GroupReadId.New, Some(block))))
  }

  /**
    * Pages through HSCAN, SSCAN, ZSCAN, or one SCAN target until the server returns a zero cursor. A filtered scan can return an empty page
    * with a non-zero cursor, so an empty page does not end iteration.
    */
  def byCursor[A](fetch: ScanCursor => CIO[ScanPage[A]]): Step[Option[ScanCursor], A] =
    resumable(cursor => fetch(cursor).map(page => (page.items, page.next)))

  /**
    * Scans every cluster target in turn, completing one node-local cursor before moving to the next. `Begin` discovers the targets, and an
    * empty target list ends the stream immediately.
    */
  def acrossTargets[A](scanTargets: CIO[Vector[ScanTarget]])(fetch: ScanTarget => ScanCursor => CIO[ScanPage[A]]): Step[ScanStep, A] = {
    case ScanStep.Begin                       =>
      scanTargets.map {
        case target +: rest => Some((Vector.empty[A], ScanStep.Visit(ScanCursor.start, target, rest)))
        case _              => None
      }
    case ScanStep.Visit(cursor, target, rest) =>
      fetch(target)(cursor).map { page =>
        page.next match {
          case Some(next) => Some((page.items, ScanStep.Visit(next, target, rest)))
          case None       =>
            rest match {
              case next +: others => Some((page.items, ScanStep.Visit(ScanCursor.start, next, others)))
              case _              => Some((page.items, ScanStep.End))
            }
        }
      }
    case ScanStep.End                         => CIO.value(None)
  }

  /**
    * XRANGE paging: advance past the last id each page; a short page (fewer than `batch`) or an empty page ends the stream.
    */
  def byRange[F, V](batch: Long)(fetch: StreamRangeId => CIO[Vector[StreamEntry[F, V]]]): Step[Option[StreamRangeId], StreamEntry[F, V]] =
    resumable(from =>
      fetch(from).map(entries => (entries, if (entries.isEmpty || entries.length < batch) None else Some(StreamRangeId.Exclusive(entries.last.id))))
    )

  /**
    * Pages through XAUTOCLAIM until its cursor returns to `StreamId.Zero`. Entries whose data has already been deleted are omitted.
    */
  def byAutoClaim[F, V](fetch: StreamId => CIO[XAutoClaimResult[F, V]]): Step[Option[StreamId], StreamEntry[F, V]] =
    resumable(from =>
      fetch(from).map(result => (result.entries.filter(_.fields.nonEmpty), Option.when(result.cursor != StreamId.Zero)(result.cursor)))
    )

  private def resumable[C, A](fetch: C => CIO[(Vector[A], Option[C])]): Step[Option[C], A] = {
    case None    => CIO.value(None)
    case Some(c) => fetch(c).map(Some(_))
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
