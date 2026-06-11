package sage.commands

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * A concrete Stream Entry ID: a millisecond timestamp and a per-millisecond sequence. The one type used for replies and for explicit
  * values; each command position that also admits a special token (`*`, `-`/`+`, `$`, `>`, `(`) carries its own sealed type listing only
  * the tokens legal there, so an illegal form at a position cannot be written. See ADR-0031.
  */
final case class StreamId(ms: Long, seq: Long) extends Ordered[StreamId] {
  def compare(that: StreamId): Int  = {
    val c = java.lang.Long.compareUnsigned(ms, that.ms); if (c != 0) c else java.lang.Long.compareUnsigned(seq, that.seq)
  }
  private[commands] def wire: Bytes = Bytes.utf8(s"$ms-$seq")
}

object StreamId {
  val Zero: StreamId = StreamId(0L, 0L)
}

/**
  * The id position of `XADD`: an explicit id, full auto (`*`), or auto-sequence within a chosen millisecond (`<ms>-*`).
  */
enum XAddId {
  case Auto
  case AutoSeq(ms: Long)
  case Explicit(id: StreamId)
}

/**
  * A bound for `XRANGE`/`XREVRANGE`/`XPENDING`: the open extremes `-`/`+`, or an inclusive/exclusive id (`(` prefix).
  */
enum StreamRangeId {
  case Min
  case Max
  case Inclusive(id: StreamId)
  case Exclusive(id: StreamId)
}

/**
  * The per-stream id of `XREAD`: only-new (`$`), the single last entry (`+`, 7.4+), or all entries after an explicit id.
  */
enum ReadId {
  case New
  case LastEntry
  case After(id: StreamId)
}

/**
  * The per-stream id of `XREADGROUP`: new-for-group (`>`), or this consumer's pending history after an explicit id.
  */
enum GroupReadId {
  case New
  case After(id: StreamId)
}

/**
  * The id of `XGROUP CREATE`/`XSETID`: the stream's last id (`$`) or an explicit id (`StreamId.Zero` is the beginning).
  */
enum GroupStartId {
  case Last
  case At(id: StreamId)
}

/**
  * One record in a Stream: an id and an ordered, duplicate-permitting list of field/value pairs (a `Vector`, never a `Map`).
  */
final case class StreamEntry[F, V](id: StreamId, fields: Vector[(F, V)])

/**
  * A trim threshold, shared by `XADD` and `XTRIM`: cap the length (`MAXLEN`) or evict ids below a floor (`MINID`).
  */
enum TrimThreshold {
  case MaxLen(count: Long)
  case MinId(id: StreamId)
}

/**
  * How a Stream is trimmed, shared by `XADD` and `XTRIM` (the [[ListSide]] domain-primitive exception). `Approximate` (`~`) is the only
  * form that admits `LIMIT`, so a `LIMIT` without `~` — which the raw command rejects — cannot be expressed.
  */
enum Trimming {
  case Exact(threshold: TrimThreshold)
  case Approximate(threshold: TrimThreshold, limit: Option[Long] = None)
}

/**
  * How entry deletion interacts with consumer-group references, shared by `XADD`/`XTRIM`/`XDELEX`/`XACKDEL` (8.2+). `KeepRef` is the
  * historical default: delete the entry but leave dangling references in every group's PEL.
  */
enum StreamDeletionPolicy {
  case KeepRef, DelRef, Acked
}

/**
  * How `XNACK` (8.8+) adjusts an entry's delivery counter when releasing it back to the group.
  */
enum NackMode {
  case Silent, Fail, Fatal
}

/**
  * The per-id outcome of `XDELEX`/`XACKDEL`: the id was absent (`-1`), deleted (`1`), or kept because references remain (`2`).
  */
enum StreamEntryDeletion {
  case NotFound, Deleted, Retained
}

/**
  * How `XCLAIM` overrides an entry's idle time: a relative duration (`IDLE`) or an absolute instant (`TIME`).
  */
enum ClaimIdle {
  case Idle(duration: FiniteDuration)
  case At(timestamp: Instant)
}

/**
  * `XAUTOCLAIM`: the next scan cursor, the claimed entries, and the ids that were dropped because they no longer exist (7.0+).
  */
final case class XAutoClaimResult[F, V](cursor: StreamId, entries: Vector[StreamEntry[F, V]], deleted: Vector[StreamId])

/**
  * `XAUTOCLAIM JUSTID`: the next cursor, the claimed ids, and the dropped ids (7.0+).
  */
final case class XAutoClaimJustIdResult(cursor: StreamId, claimed: Vector[StreamId], deleted: Vector[StreamId])

/**
  * `XPENDING` summary: total pending, the id range, and the per-consumer counts. All empty when nothing is pending.
  */
final case class PendingSummary(total: Long, min: Option[StreamId], max: Option[StreamId], consumers: Vector[(String, Long)])

/**
  * One row of `XPENDING` extended: the entry, its owning consumer, how long it has been idle, and how many times it was delivered.
  */
final case class PendingEntry(id: StreamId, consumer: String, idle: FiniteDuration, deliveryCount: Long)

private[sage] object Streams {

  private val Star         = Bytes.utf8("*")
  private val Dash         = Bytes.utf8("-")
  private val Plus         = Bytes.utf8("+")
  private val Dollar       = Bytes.utf8("$")
  private val Gt           = Bytes.utf8(">")
  private val NoMkStream   = Bytes.utf8("NOMKSTREAM")
  private val MaxLenWord   = Bytes.utf8("MAXLEN")
  private val MinIdWord    = Bytes.utf8("MINID")
  private val Eq           = Bytes.utf8("=")
  private val Tilde        = Bytes.utf8("~")
  private val LimitWord    = Bytes.utf8("LIMIT")
  private val Count        = Bytes.utf8("COUNT")
  private val Block        = Bytes.utf8("BLOCK")
  private val StreamsWord  = Bytes.utf8("STREAMS")
  private val Group        = Bytes.utf8("GROUP")
  private val NoAck        = Bytes.utf8("NOACK")
  private val MkStream     = Bytes.utf8("MKSTREAM")
  private val EntriesRead  = Bytes.utf8("ENTRIESREAD")
  private val EntriesAdded = Bytes.utf8("ENTRIESADDED")
  private val MaxDeletedId = Bytes.utf8("MAXDELETEDID")
  private val Idle         = Bytes.utf8("IDLE")
  private val Time         = Bytes.utf8("TIME")
  private val RetryCount   = Bytes.utf8("RETRYCOUNT")
  private val Force        = Bytes.utf8("FORCE")
  private val JustId       = Bytes.utf8("JUSTID")
  private val Ids          = Bytes.utf8("IDS")
  private val DelRef       = Bytes.utf8("DELREF")
  private val Acked        = Bytes.utf8("ACKED")
  private val Silent       = Bytes.utf8("SILENT")
  private val Fail         = Bytes.utf8("FAIL")
  private val Fatal        = Bytes.utf8("FATAL")
  private val IdmpDuration = Bytes.utf8("IDMP-DURATION")
  private val IdmpMaxSize  = Bytes.utf8("IDMP-MAXSIZE")

  // --- writes -------------------------------------------------------------

  def xAdd[K, F, V](key: K, id: XAddId = XAddId.Auto, trim: Option[Trimming] = None, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: (F, V),
    rest: (F, V)*
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[StreamId] =
    Command("XADD", Command.FirstKey, addArgs(key, noMkStream = false, id, trim, policy, first +: rest.toVector), streamId)

  // returns None when NOMKSTREAM was set and the stream did not exist
  def xAddNoMkStream[K, F, V](
    key: K,
    id: XAddId = XAddId.Auto,
    trim: Option[Trimming] = None,
    policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef
  )(first: (F, V), rest: (F, V)*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Option[StreamId]] =
    Command("XADD", Command.FirstKey, addArgs(key, noMkStream = true, id, trim, policy, first +: rest.toVector), optionalStreamId)

  def xLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("XLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def xDel[K](key: K)(first: StreamId, rest: StreamId*)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("XDEL", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(_.wire), Decode.long)

  def xTrim[K](key: K, trim: Trimming, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("XTRIM", Command.FirstKey, (keyCodec.encode(key) +: trimArgs(trim)) ++ policyArgs(policy), Decode.long)

  def xSetId[K](key: K, id: GroupStartId, entriesAdded: Option[Long] = None, maxDeletedId: Option[StreamId] = None)(
    using keyCodec: KeyCodec[K]
  ): Command[Unit] =
    Command(
      "XSETID",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), groupStartWire(id)) ++ entriesAdded.toVector.flatMap(n => Vector(EntriesAdded, Bytes.utf8(n.toString)))) ++
        maxDeletedId.toVector.flatMap(d => Vector(MaxDeletedId, d.wire)),
      Decode.ok
    )

  // sets per-stream idempotent-message-processing config; the server rejects an empty option set, so neither is required here (ADR-0026)
  def xCfgSet[K](key: K, idmpDuration: Option[FiniteDuration] = None, idmpMaxSize: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Unit] =
    Command(
      "XCFGSET",
      Command.FirstKey,
      keyCodec.encode(key) +:
        (idmpDuration.toVector.flatMap(d => Vector(IdmpDuration, Bytes.utf8(d.toSeconds.toString))) ++
          idmpMaxSize.toVector.flatMap(n => Vector(IdmpMaxSize, Bytes.utf8(n.toString)))),
      Decode.ok
    )

  // --- range reads --------------------------------------------------------

  def xRange[K, F, V](key: K, start: StreamRangeId = StreamRangeId.Min, end: StreamRangeId = StreamRangeId.Max, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[StreamEntry[F, V]]] =
    rangeCommand("XRANGE", key, start, end, count)

  def xRevRange[K, F, V](key: K, end: StreamRangeId = StreamRangeId.Max, start: StreamRangeId = StreamRangeId.Min, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[StreamEntry[F, V]]] =
    rangeCommand("XREVRANGE", key, end, start, count)

  private def rangeCommand[K, F, V](name: String, key: K, a: StreamRangeId, b: StreamRangeId, count: Option[Long])(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[StreamEntry[F, V]]] =
    Command.read(
      name,
      Command.FirstKey,
      Vector(keyCodec.encode(key), rangeWire(a), rangeWire(b)) ++ count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString))),
      Decode.vector(streamEntry[F, V])
    )

  // --- multi-stream reads -------------------------------------------------

  def xRead[K, F, V](first: (K, ReadId), rest: (K, ReadId)*)(count: Option[Long] = None, block: Option[BlockTimeout] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val leading      = countArg(count) ++ blockArg(block)
    val (args, keys) = streamsArgs(first +: rest.toVector, leading, readWire)
    Command("XREAD", keys, args, readReply[K, F, V], execution = blockExecution(block), isReadOnly = true)
  }

  def xReadGroup[K, F, V](group: String, consumer: String)(first: (K, GroupReadId), rest: (K, GroupReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None,
    noAck: Boolean = false
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val leading      =
      Vector(Group, Bytes.utf8(group), Bytes.utf8(consumer)) ++ countArg(count) ++ blockArg(block) ++ (if (noAck) Vector(NoAck) else Vector.empty)
    val (args, keys) = streamsArgs(first +: rest.toVector, leading, groupReadWire)
    Command("XREADGROUP", keys, args, readReply[K, F, V], execution = blockExecution(block))
  }

  // --- consumer groups ----------------------------------------------------

  def xAck[K](key: K, group: String)(first: StreamId, rest: StreamId*)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("XACK", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(group)) ++ (first +: rest.toVector).map(_.wire), Decode.long)

  def xGroupCreate[K](key: K, group: String, id: GroupStartId = GroupStartId.Last, mkStream: Boolean = false, entriesRead: Option[Long] = None)(
    using keyCodec: KeyCodec[K]
  ): Command[Unit] =
    Command(
      "XGROUP CREATE",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group), groupStartWire(id)) ++ (if (mkStream) Vector(MkStream) else Vector.empty)) ++
        entriesRead.toVector.flatMap(n => Vector(EntriesRead, Bytes.utf8(n.toString))),
      Decode.ok
    )

  def xGroupSetId[K](key: K, group: String, id: GroupStartId = GroupStartId.Last, entriesRead: Option[Long] = None)(
    using keyCodec: KeyCodec[K]
  ): Command[Unit] =
    Command(
      "XGROUP SETID",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(group), groupStartWire(id)) ++ entriesRead.toVector.flatMap(n =>
        Vector(EntriesRead, Bytes.utf8(n.toString))
      ),
      Decode.ok
    )

  def xGroupDestroy[K](key: K, group: String)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("XGROUP DESTROY", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(group)), Decode.flag)

  def xGroupCreateConsumer[K](key: K, group: String, consumer: String)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("XGROUP CREATECONSUMER", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer)), Decode.flag)

  def xGroupDelConsumer[K](key: K, group: String, consumer: String)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("XGROUP DELCONSUMER", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer)), Decode.long)

  // --- claiming -----------------------------------------------------------

  def xClaim[K, F, V](key: K, group: String, consumer: String, minIdle: FiniteDuration)(first: StreamId, rest: StreamId*)(
    idle: Option[ClaimIdle] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[StreamEntry[F, V]]] =
    Command(
      "XCLAIM",
      Command.FirstKey,
      claimArgs(key, group, consumer, minIdle, first +: rest.toVector, idle, retryCount, force, justId = false),
      Decode.vector(streamEntry[F, V])
    )

  def xClaimJustId[K](key: K, group: String, consumer: String, minIdle: FiniteDuration)(first: StreamId, rest: StreamId*)(
    idle: Option[ClaimIdle] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(using keyCodec: KeyCodec[K]): Command[Vector[StreamId]] =
    Command(
      "XCLAIM",
      Command.FirstKey,
      claimArgs(key, group, consumer, minIdle, first +: rest.toVector, idle, retryCount, force, justId = true),
      Decode.vector(streamId)
    )

  def xAutoClaim[K, F, V](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  )(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[XAutoClaimResult[F, V]] =
    Command("XAUTOCLAIM", Command.FirstKey, autoClaimArgs(key, group, consumer, minIdle, start, count, justId = false), autoClaimReply[F, V])

  def xAutoClaimJustId[K](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  )(
    using keyCodec: KeyCodec[K]
  ): Command[XAutoClaimJustIdResult] =
    Command("XAUTOCLAIM", Command.FirstKey, autoClaimArgs(key, group, consumer, minIdle, start, count, justId = true), autoClaimJustIdReply)

  // --- pending ------------------------------------------------------------

  def xPending[K](key: K, group: String)(using keyCodec: KeyCodec[K]): Command[PendingSummary] =
    Command.readUncacheable("XPENDING", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(group)), pendingSummaryReply)

  def xPendingExtended[K](
    key: K,
    group: String,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    count: Long = 10L,
    consumer: Option[String] = None,
    idle: Option[FiniteDuration] = None
  )(using keyCodec: KeyCodec[K]): Command[Vector[PendingEntry]] =
    Command.readUncacheable(
      "XPENDING",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group)) ++ idle.toVector.flatMap(d => Vector(Idle, Bytes.utf8(TimeArgs.millis(d).toString)))) ++
        Vector(rangeWire(start), rangeWire(end), Bytes.utf8(count.toString)) ++ consumer.toVector.map(Bytes.utf8),
      Decode.vector(pendingEntryElement)
    )

  // --- Redis-only 8.2+/8.8+ (ADR-0026) ------------------------------------

  def xDelEx[K](key: K, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(first: StreamId, rest: StreamId*)(
    using keyCodec: KeyCodec[K]
  ): Command[Vector[StreamEntryDeletion]] = {
    val ids = first +: rest.toVector
    Command(
      "XDELEX",
      Command.FirstKey,
      (keyCodec.encode(key) +: policyArgs(policy)) ++ (Ids +: Bytes.utf8(ids.size.toString) +: ids.map(_.wire)),
      Decode.vector(deletionElement)
    )
  }

  def xAckDel[K](key: K, group: String, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(first: StreamId, rest: StreamId*)(
    using keyCodec: KeyCodec[K]
  ): Command[Vector[StreamEntryDeletion]] = {
    val ids = first +: rest.toVector
    Command(
      "XACKDEL",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group)) ++ policyArgs(policy)) ++ (Ids +: Bytes.utf8(ids.size.toString) +: ids.map(_.wire)),
      Decode.vector(deletionElement)
    )
  }

  def xNack[K](key: K, group: String, mode: NackMode)(first: StreamId, rest: StreamId*)(
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(using keyCodec: KeyCodec[K]): Command[Long] = {
    val ids = first +: rest.toVector
    Command(
      "XNACK",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group), nackModeWire(mode), Ids, Bytes.utf8(ids.size.toString)) ++ ids.map(_.wire)) ++
        retryCount.toVector.flatMap(n => Vector(RetryCount, Bytes.utf8(n.toString))) ++ (if (force) Vector(Force) else Vector.empty),
      Decode.long
    )
  }

  // --- arg builders -------------------------------------------------------

  private def addArgs[K, F, V](key: K, noMkStream: Boolean, id: XAddId, trim: Option[Trimming], policy: StreamDeletionPolicy, fields: Vector[(F, V)])(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Vector[Bytes] =
    (keyCodec.encode(key) +: (if (noMkStream) Vector(NoMkStream) else Vector.empty)) ++
      policyArgs(policy) ++ trim.toVector.flatMap(trimArgs) ++ (xAddIdWire(id) +: fields.flatMap { case (f, v) =>
        Vector(fieldCodec.encode(f), valueCodec.encode(v))
      })

  private def claimArgs[K](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    ids: Vector[StreamId],
    idle: Option[ClaimIdle],
    retryCount: Option[Long],
    force: Boolean,
    justId: Boolean
  )(using keyCodec: KeyCodec[K]): Vector[Bytes] =
    (Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer), Bytes.utf8(TimeArgs.millis(minIdle).toString)) ++ ids.map(_.wire)) ++
      idleArgs(idle) ++ retryCount.toVector.flatMap(n => Vector(RetryCount, Bytes.utf8(n.toString))) ++
      (if (force) Vector(Force) else Vector.empty) ++ (if (justId) Vector(JustId) else Vector.empty)

  private def autoClaimArgs[K](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId,
    count: Option[Long],
    justId: Boolean
  )(
    using keyCodec: KeyCodec[K]
  ): Vector[Bytes] =
    Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer), Bytes.utf8(TimeArgs.millis(minIdle).toString), start.wire) ++
      count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString))) ++ (if (justId) Vector(JustId) else Vector.empty)

  private def idleArgs(idle: Option[ClaimIdle]): Vector[Bytes] =
    idle.toVector.flatMap {
      case ClaimIdle.Idle(duration) => Vector(Idle, Bytes.utf8(TimeArgs.millis(duration).toString))
      case ClaimIdle.At(timestamp)  => Vector(Time, Bytes.utf8(TimeArgs.millis(timestamp).toString))
    }

  private def trimArgs(trim: Trimming): Vector[Bytes] =
    trim match {
      case Trimming.Exact(threshold)              => thresholdKeyword(threshold) +: Vector(Eq, thresholdValue(threshold))
      case Trimming.Approximate(threshold, limit) =>
        (thresholdKeyword(threshold) +: Vector(Tilde, thresholdValue(threshold))) ++ limit.toVector.flatMap(n =>
          Vector(LimitWord, Bytes.utf8(n.toString))
        )
    }

  private def thresholdKeyword(threshold: TrimThreshold): Bytes = threshold match {
    case _: TrimThreshold.MaxLen => MaxLenWord; case _: TrimThreshold.MinId => MinIdWord
  }
  private def thresholdValue(threshold: TrimThreshold): Bytes   = threshold match {
    case TrimThreshold.MaxLen(c) => Bytes.utf8(c.toString); case TrimThreshold.MinId(id) => id.wire
  }

  private def policyArgs(policy: StreamDeletionPolicy): Vector[Bytes] =
    policy match {
      case StreamDeletionPolicy.KeepRef => Vector.empty
      case StreamDeletionPolicy.DelRef  => Vector(DelRef)
      case StreamDeletionPolicy.Acked   => Vector(Acked)
    }

  private def streamsArgs[K, I](keysAndIds: Vector[(K, I)], leading: Vector[Bytes], idWire: I => Bytes)(
    using keyCodec: KeyCodec[K]
  ): (Vector[Bytes], Vector[Int]) = {
    val keys       = keysAndIds.map { case (key, _) => keyCodec.encode(key) }
    val ids        = keysAndIds.map { case (_, id) => idWire(id) }
    val keyStart   = leading.length + 1 // after the leading options and the STREAMS keyword
    val keyIndices = Vector.tabulate(keys.length)(keyStart + _)
    ((leading :+ StreamsWord) ++ keys ++ ids, keyIndices)
  }

  private def countArg(count: Option[Long]): Vector[Bytes]           = count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString)))
  private def blockArg(block: Option[BlockTimeout]): Vector[Bytes]   = block.toVector.flatMap(b => Vector(Block, BlockTimeout.millisWire(b)))
  private def blockExecution(block: Option[BlockTimeout]): Execution = if (block.isDefined) Execution.Blocking else Execution.Ordinary

  // --- token wire forms ---------------------------------------------------

  private def xAddIdWire(id: XAddId): Bytes =
    id match {
      case XAddId.Auto          => Star
      case XAddId.AutoSeq(ms)   => Bytes.utf8(s"$ms-*")
      case XAddId.Explicit(sid) => sid.wire
    }

  private def rangeWire(id: StreamRangeId): Bytes =
    id match {
      case StreamRangeId.Min            => Dash
      case StreamRangeId.Max            => Plus
      case StreamRangeId.Inclusive(sid) => sid.wire
      case StreamRangeId.Exclusive(sid) => Bytes.utf8(s"(${sid.ms}-${sid.seq}")
    }

  private def readWire(id: ReadId): Bytes =
    id match {
      case ReadId.New        => Dollar
      case ReadId.LastEntry  => Plus
      case ReadId.After(sid) => sid.wire
    }

  private def groupReadWire(id: GroupReadId): Bytes =
    id match {
      case GroupReadId.New        => Gt
      case GroupReadId.After(sid) => sid.wire
    }

  private def groupStartWire(id: GroupStartId): Bytes =
    id match {
      case GroupStartId.Last    => Dollar
      case GroupStartId.At(sid) => sid.wire
    }

  private def nackModeWire(mode: NackMode): Bytes =
    mode match {
      case NackMode.Silent => Silent
      case NackMode.Fail   => Fail
      case NackMode.Fatal  => Fatal
    }

  // --- decoders -----------------------------------------------------------

  private[commands] val streamId: Frame => Either[DecodeError, StreamId] = {
    case Frame.BulkString(raw)   => parseId(raw.asUtf8String)
    case Frame.SimpleString(raw) => parseId(raw)
    case other                   => Left(DecodeError("stream id", Frame.describe(other)))
  }

  private def parseId(text: String): Either[DecodeError, StreamId] = {
    val dash = text.indexOf('-')
    if (dash < 0) text.toLongOption.map(ms => StreamId(ms, 0L)).toRight(DecodeError("stream id 'ms-seq'", s"'$text'"))
    else
      (text.substring(0, dash).toLongOption, text.substring(dash + 1).toLongOption) match {
        case (Some(ms), Some(seq)) => Right(StreamId(ms, seq))
        case _                     => Left(DecodeError("stream id 'ms-seq'", s"'$text'"))
      }
  }

  private val optionalStreamId: Frame => Either[DecodeError, Option[StreamId]] = {
    case Frame.Null => Right(None)
    case other      => streamId(other).map(Some(_))
  }

  // an entry is `[id, [field, value, …]]`; a tombstone (claimed entry whose data was deleted) is `[id, nil]`
  private[commands] def streamEntry[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, StreamEntry[F, V]] = {
    case Frame.Array(Vector(idFrame, fieldsFrame)) =>
      for {
        id     <- streamId(idFrame)
        fields <- fieldsFrame match {
                    case Frame.Null => Right(Vector.empty[(F, V)])
                    case other      => Decode.flatPairs[F, V](other)
                  }
      } yield StreamEntry(id, fields)
    case other                                     => Left(DecodeError("stream entry [id, fields]", Frame.describe(other)))
  }

  // XREAD/XREADGROUP reply: RESP3 map of stream-name -> entries (RESP2 array of [name, entries] pairs); null when nothing is ready
  private def readReply[K, F, V](
    using KeyCodec[K],
    KeyCodec[F],
    ValueCodec[V]
  ): Frame => Either[DecodeError, Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val entries                                                                                          = Decode.vector(streamEntry[F, V])
    def pair(nameFrame: Frame, entriesFrame: Frame): Either[DecodeError, (K, Vector[StreamEntry[F, V]])] =
      for {
        name    <- Decode.key[K](nameFrame)
        decoded <- entries(entriesFrame)
      } yield name -> decoded
    frame =>
      frame match {
        case Frame.Null        => Right(Vector.empty)
        case Frame.Map(rows)   => collectPairs(rows) { case (n, e) => pair(n, e) }
        case Frame.Array(rows) =>
          collect(rows) {
            case Frame.Array(Vector(n, e)) => pair(n, e)
            case other                     => Left(DecodeError("stream [name, entries] pair", Frame.describe(other)))
          }
        case other             => Left(DecodeError("stream read map or null", Frame.describe(other)))
      }
  }

  private def autoClaimReply[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, XAutoClaimResult[F, V]] = {
    case Frame.Array(Vector(cursorFrame, entriesFrame, deletedFrame)) =>
      for {
        cursor  <- streamId(cursorFrame)
        entries <- Decode.vector(streamEntry[F, V])(entriesFrame)
        deleted <- Decode.vector(streamId)(deletedFrame)
      } yield XAutoClaimResult(cursor, entries, deleted)
    // pre-7.0 omits the deleted-ids element
    case Frame.Array(Vector(cursorFrame, entriesFrame))               =>
      for {
        cursor  <- streamId(cursorFrame)
        entries <- Decode.vector(streamEntry[F, V])(entriesFrame)
      } yield XAutoClaimResult(cursor, entries, Vector.empty)
    case other                                                        => Left(DecodeError("xautoclaim [cursor, entries, deleted]", Frame.describe(other)))
  }

  private val autoClaimJustIdReply: Frame => Either[DecodeError, XAutoClaimJustIdResult] = {
    case Frame.Array(Vector(cursorFrame, claimedFrame, deletedFrame)) =>
      for {
        cursor  <- streamId(cursorFrame)
        claimed <- Decode.vector(streamId)(claimedFrame)
        deleted <- Decode.vector(streamId)(deletedFrame)
      } yield XAutoClaimJustIdResult(cursor, claimed, deleted)
    case Frame.Array(Vector(cursorFrame, claimedFrame))               =>
      for {
        cursor  <- streamId(cursorFrame)
        claimed <- Decode.vector(streamId)(claimedFrame)
      } yield XAutoClaimJustIdResult(cursor, claimed, Vector.empty)
    case other                                                        => Left(DecodeError("xautoclaim justid [cursor, ids, deleted]", Frame.describe(other)))
  }

  private val deletionElement: Frame => Either[DecodeError, StreamEntryDeletion] = {
    case Frame.Integer(-1L) => Right(StreamEntryDeletion.NotFound)
    case Frame.Integer(1L)  => Right(StreamEntryDeletion.Deleted)
    case Frame.Integer(2L)  => Right(StreamEntryDeletion.Retained)
    case other              => Left(DecodeError("deletion status -1/1/2", Frame.describe(other)))
  }

  // XPENDING summary: [total, min-id, max-id, [[consumer, count], …]]; an empty group replies [0, nil, nil, nil]
  private val pendingSummaryReply: Frame => Either[DecodeError, PendingSummary] = {
    case Frame.Array(Vector(totalFrame, minFrame, maxFrame, consumersFrame)) =>
      for {
        total     <- Decode.long(totalFrame)
        min       <- optionalStreamId(minFrame)
        max       <- optionalStreamId(maxFrame)
        consumers <- consumersFrame match {
                       case Frame.Null        => Right(Vector.empty[(String, Long)])
                       case Frame.Array(rows) => collect(rows)(consumerCount)
                       case other             => Left(DecodeError("consumer counts array or null", Frame.describe(other)))
                     }
      } yield PendingSummary(total, min, max, consumers)
    case other                                                               => Left(DecodeError("xpending summary", Frame.describe(other)))
  }

  private val consumerCount: Frame => Either[DecodeError, (String, Long)] = {
    case Frame.Array(Vector(nameFrame, countFrame)) =>
      for {
        name  <- Decode.utf8String(nameFrame)
        count <- countText(countFrame)
      } yield name -> count
    case other                                      => Left(DecodeError("[consumer, count] pair", Frame.describe(other)))
  }

  // XPENDING extended row: [id, consumer, idle-ms, delivery-count]
  private val pendingEntryElement: Frame => Either[DecodeError, PendingEntry] = {
    case Frame.Array(Vector(idFrame, consumerFrame, idleFrame, deliveriesFrame)) =>
      for {
        id         <- streamId(idFrame)
        consumer   <- Decode.utf8String(consumerFrame)
        idle       <- Decode.long(idleFrame)
        deliveries <- Decode.long(deliveriesFrame)
      } yield PendingEntry(id, consumer, FiniteDuration(idle, java.util.concurrent.TimeUnit.MILLISECONDS), deliveries)
    case other                                                                   => Left(DecodeError("xpending entry [id, consumer, idle, count]", Frame.describe(other)))
  }

  // XPENDING per-consumer counts come back as bulk-string integers
  private def countText(frame: Frame): Either[DecodeError, Long] =
    frame match {
      case Frame.Integer(value)    => Right(value)
      case Frame.BulkString(bytes) => bytes.asUtf8String.toLongOption.toRight(DecodeError("integer", s"bulk string '${bytes.asUtf8String}'"))
      case other                   => Left(DecodeError("integer", Frame.describe(other)))
    }

  private def collect[A](frames: Vector[Frame])(f: Frame => Either[DecodeError, A]): Either[DecodeError, Vector[A]] = {
    val builder = Vector.newBuilder[A]
    builder.sizeHint(frames.length)
    val it      = frames.iterator
    while (it.hasNext)
      f(it.next()) match {
        case Right(value) => builder += value
        case Left(error)  => return Left(error)
      }
    Right(builder.result())
  }

  private def collectPairs[A](pairs: Vector[(Frame, Frame)])(f: (Frame, Frame) => Either[DecodeError, A]): Either[DecodeError, Vector[A]] = {
    val builder = Vector.newBuilder[A]
    builder.sizeHint(pairs.length)
    val it      = pairs.iterator
    while (it.hasNext) {
      val (a, b) = it.next()
      f(a, b) match {
        case Right(value) => builder += value
        case Left(error)  => return Left(error)
      }
    }
    Right(builder.result())
  }
}
