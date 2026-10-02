package sage.commands

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, Primitives, ValueCodec}
import sage.commands.Args.{Count, LimitWord}
import sage.protocol.Frame

/**
  * A concrete stream-entry ID containing a millisecond timestamp and sequence number. Replies and explicit IDs use this type. Command
  * positions that accept special tokens (`*`, `-`/`+`, `$`, `>`, `(`) use separate sealed types that list the supported values.
  */
final case class StreamId(ms: Long, seq: Long) extends Ordered[StreamId] {
  def compare(that: StreamId): Int   = {
    val c = java.lang.Long.compareUnsigned(ms, that.ms)
    if (c != 0) c else java.lang.Long.compareUnsigned(seq, that.seq)
  }
  private[commands] def text: String = s"${java.lang.Long.toUnsignedString(ms)}-${java.lang.Long.toUnsignedString(seq)}"
  private[commands] def wire: Bytes  = Bytes.utf8(text)
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
  * Trimming options shared by `XADD` and `XTRIM`. Only `Approximate`, represented by `~`, accepts `LIMIT`.
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
  private val policyWord   = Args.keywords(StreamDeletionPolicy.values)
  private val nackModeWire = Args.keywords(NackMode.values)
  private val IdmpDuration = Bytes.utf8("IDMP-DURATION")
  private val IdmpMaxSize  = Bytes.utf8("IDMP-MAXSIZE")

  // --- writes -------------------------------------------------------------

  def xAdd[K, F, V](key: K, id: XAddId = XAddId.Auto, trim: Option[Trimming] = None, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: (F, V),
    rest: (F, V)*
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[StreamId] =
    Command("XADD", Command.FirstKey, addArgs(key, noMkStream = false, id, trim, policy, first +: rest.toVector), streamId)

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
    Command("XDEL", Command.FirstKey, Args.keyThen(key, first, rest)(_.wire), Decode.long)

  def xTrim[K](key: K, trim: Trimming, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("XTRIM", Command.FirstKey, (keyCodec.encode(key) +: trimArgs(trim)) ++ policyArgs(policy), Decode.long)

  def xSetId[K](key: K, id: GroupStartId, entriesAdded: Option[Long] = None, maxDeletedId: Option[StreamId] = None)(
    using keyCodec: KeyCodec[K]
  ): Command[Unit] =
    Command(
      "XSETID",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), groupStartWire(id)) ++ Args.optLong(EntriesAdded, entriesAdded)) ++
        Args.opt(MaxDeletedId, maxDeletedId)(_.wire),
      Decode.ok
    )

  // sets per-stream idempotent-message-processing config; the server rejects an empty option set, so neither is required here
  def xCfgSet[K](key: K, idmpDuration: Option[FiniteDuration] = None, idmpMaxSize: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Unit] =
    Command(
      "XCFGSET",
      Command.FirstKey,
      keyCodec.encode(key) +:
        (Args.opt(IdmpDuration, idmpDuration)(d => Args.long(Math.ceilDiv(d.toNanos, 1000000000L))) ++
          Args.optLong(IdmpMaxSize, idmpMaxSize)),
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
      Vector(keyCodec.encode(key), rangeWire(a), rangeWire(b)) ++ Args.optLong(Count, count),
      Decode.vector(streamEntry[F, V])
    )

  // --- multi-stream reads -------------------------------------------------

  def xRead[K, F, V](first: (K, ReadId), rest: (K, ReadId)*)(count: Option[Long] = None, block: Option[BlockTimeout] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val leading      = Args.optLong(Count, count) ++ blockArg(block)
    val (args, keys) = streamsArgs(first +: rest.toVector, leading, readWire)
    Command("XREAD", keys, args, readReply[K, F, V], execution = blockExecution(block), isReadOnly = true)
  }

  def xReadGroup[K, F, V](group: String, consumer: String)(first: (K, GroupReadId), rest: (K, GroupReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None,
    noAck: Boolean = false
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val leading      =
      Vector(Group, Bytes.utf8(group), Bytes.utf8(consumer)) ++ Args.optLong(Count, count) ++ blockArg(block) ++ Args.flag(noAck, NoAck)
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
      (Vector(keyCodec.encode(key), Bytes.utf8(group), groupStartWire(id)) ++ Args.flag(mkStream, MkStream)) ++
        Args.optLong(EntriesRead, entriesRead),
      Decode.ok
    )

  def xGroupSetId[K](key: K, group: String, id: GroupStartId = GroupStartId.Last, entriesRead: Option[Long] = None)(
    using keyCodec: KeyCodec[K]
  ): Command[Unit] =
    Command(
      "XGROUP SETID",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(group), groupStartWire(id)) ++ Args.optLong(EntriesRead, entriesRead),
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
      claimArgs(key, group, consumer, minIdle, first +: rest.toVector, idle, retryCount, force),
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
      claimArgs(key, group, consumer, minIdle, first +: rest.toVector, idle, retryCount, force) :+ JustId,
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
    Command("XAUTOCLAIM", Command.FirstKey, autoClaimArgs(key, group, consumer, minIdle, start, count), autoClaimReply[F, V])

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
    Command("XAUTOCLAIM", Command.FirstKey, autoClaimArgs(key, group, consumer, minIdle, start, count) :+ JustId, autoClaimJustIdReply)

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
      (Vector(keyCodec.encode(key), Bytes.utf8(group)) ++ Args.opt(Idle, idle)(d => Args.long(TimeArgs.millis(d)))) ++
        Vector(rangeWire(start), rangeWire(end), Args.long(count)) ++ consumer.toVector.map(Bytes.utf8),
      Decode.vector(pendingEntryElement)
    )

  // --- Redis-only 8.2+/8.8+ ------------------------------------

  def xDelEx[K](key: K, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(first: StreamId, rest: StreamId*)(
    using keyCodec: KeyCodec[K]
  ): Command[Vector[StreamEntryDeletion]] =
    Command("XDELEX", Command.FirstKey, (keyCodec.encode(key) +: policyArgs(policy)) ++ idsArgs(first, rest), Decode.vector(deletionElement))

  def xAckDel[K](key: K, group: String, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(first: StreamId, rest: StreamId*)(
    using keyCodec: KeyCodec[K]
  ): Command[Vector[StreamEntryDeletion]] =
    Command(
      "XACKDEL",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group)) ++ policyArgs(policy)) ++ idsArgs(first, rest),
      Decode.vector(deletionElement)
    )

  def xNack[K](key: K, group: String, mode: NackMode)(first: StreamId, rest: StreamId*)(
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(using keyCodec: KeyCodec[K]): Command[Long] =
    Command(
      "XNACK",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), Bytes.utf8(group), nackModeWire(mode)) ++ idsArgs(first, rest)) ++
        Args.optLong(RetryCount, retryCount) ++ Args.flag(force, Force),
      Decode.long
    )

  // --- arg builders -------------------------------------------------------

  private def addArgs[K, F, V](key: K, noMkStream: Boolean, id: XAddId, trim: Option[Trimming], policy: StreamDeletionPolicy, fields: Vector[(F, V)])(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Vector[Bytes] =
    (keyCodec.encode(key) +: Args.flag(noMkStream, NoMkStream)) ++
      policyArgs(policy) ++ trim.toVector.flatMap(trimArgs) ++ (xAddIdWire(id) +: Args.pairs(fields))

  private def claimArgs[K](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    ids: Vector[StreamId],
    idle: Option[ClaimIdle],
    retryCount: Option[Long],
    force: Boolean
  )(using keyCodec: KeyCodec[K]): Vector[Bytes] =
    (Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer), Args.long(TimeArgs.millis(minIdle))) ++ ids.map(_.wire)) ++
      idleArgs(idle) ++ Args.optLong(RetryCount, retryCount) ++
      Args.flag(force, Force)

  private def autoClaimArgs[K](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId,
    count: Option[Long]
  )(
    using keyCodec: KeyCodec[K]
  ): Vector[Bytes] =
    Vector(keyCodec.encode(key), Bytes.utf8(group), Bytes.utf8(consumer), Args.long(TimeArgs.millis(minIdle)), start.wire) ++
      Args.optLong(Count, count)

  private def idsArgs(first: StreamId, rest: Seq[StreamId]): Vector[Bytes] =
    Ids +: Args.long(rest.length + 1) +: (first +: rest.toVector).map(_.wire)

  private def idleArgs(idle: Option[ClaimIdle]): Vector[Bytes] =
    idle.toVector.flatMap {
      case ClaimIdle.Idle(duration) => Vector(Idle, Args.long(TimeArgs.millis(duration)))
      case ClaimIdle.At(timestamp)  => Vector(Time, Args.long(TimeArgs.millis(timestamp)))
    }

  private def trimArgs(trim: Trimming): Vector[Bytes] =
    trim match {
      case Trimming.Exact(threshold)              => thresholdArgs(threshold, Eq)
      case Trimming.Approximate(threshold, limit) => thresholdArgs(threshold, Tilde) ++ Args.optLong(LimitWord, limit)
    }

  private def thresholdArgs(threshold: TrimThreshold, operator: Bytes): Vector[Bytes] =
    threshold match {
      case TrimThreshold.MaxLen(count) => Vector(MaxLenWord, operator, Args.long(count))
      case TrimThreshold.MinId(id)     => Vector(MinIdWord, operator, id.wire)
    }

  private def policyArgs(policy: StreamDeletionPolicy): Vector[Bytes] =
    if (policy == StreamDeletionPolicy.KeepRef) Vector.empty else Vector(policyWord(policy))

  private def streamsArgs[K, I](keysAndIds: Vector[(K, I)], leading: Vector[Bytes], idWire: I => Bytes)(
    using keyCodec: KeyCodec[K]
  ): (Vector[Bytes], Vector[Int]) = {
    val keys       = keysAndIds.map { case (key, _) => keyCodec.encode(key) }
    val ids        = keysAndIds.map { case (_, id) => idWire(id) }
    val keyStart   = leading.length + 1 // after the leading options and the STREAMS keyword
    val keyIndices = Vector.tabulate(keys.length)(keyStart + _)
    ((leading :+ StreamsWord) ++ keys ++ ids, keyIndices)
  }

  private def blockArg(block: Option[BlockTimeout]): Vector[Bytes]   = Args.opt(Block, block)(BlockTimeout.millisWire)
  private def blockExecution(block: Option[BlockTimeout]): Execution = if (block.isDefined) Execution.Blocking else Execution.Ordinary

  // --- token wire forms ---------------------------------------------------

  private def xAddIdWire(id: XAddId): Bytes =
    id match {
      case XAddId.Auto          => Star
      case XAddId.AutoSeq(ms)   => Bytes.utf8(s"${java.lang.Long.toUnsignedString(ms)}-*")
      case XAddId.Explicit(sid) => sid.wire
    }

  private def rangeWire(id: StreamRangeId): Bytes =
    id match {
      case StreamRangeId.Min            => Dash
      case StreamRangeId.Max            => Plus
      case StreamRangeId.Inclusive(sid) => sid.wire
      case StreamRangeId.Exclusive(sid) => Bytes.utf8("(" + sid.text)
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

  // --- decoders -----------------------------------------------------------

  private[commands] val streamId: Frame => Either[DecodeError, StreamId] = Decode.shape("stream id") {
    case Frame.BulkString(raw)   => parseId(raw.asUtf8String)
    case Frame.SimpleString(raw) => parseId(raw)
  }

  // both parts are unsigned 64-bit numbers, matching StreamId.compare
  private def parseId(text: String): Either[DecodeError, StreamId] = {
    val dash = text.indexOf('-')
    val id   =
      if (dash < 0) unsignedLong(text).map(StreamId(_, 0L))
      else unsignedLong(text.substring(0, dash)).zip(unsignedLong(text.substring(dash + 1))).map(StreamId(_, _))
    id.toRight(DecodeError("stream id 'ms-seq'", s"'$text'"))
  }

  private def unsignedLong(text: String): Option[Long] =
    try Some(java.lang.Long.parseUnsignedLong(text))
    catch { case _: NumberFormatException => None }

  private val optionalStreamId: Frame => Either[DecodeError, Option[StreamId]] = Decode.nullable(streamId)

  // an entry is `[id, [field, value, …]]`; a tombstone (claimed entry whose data was deleted) is `[id, nil]`
  private[commands] def streamEntry[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, StreamEntry[F, V]] =
    Decode.array2(streamId, Decode.orEmpty(Decode.flatPairs[F, V]), "stream entry [id, fields]")(StreamEntry(_, _))

  // XREAD/XREADGROUP reply: RESP3 map of stream-name -> entries (RESP2 array of [name, entries] pairs); null when nothing is ready
  private def readReply[K, F, V](
    using KeyCodec[K],
    KeyCodec[F],
    ValueCodec[V]
  ): Frame => Either[DecodeError, Vector[(K, Vector[StreamEntry[F, V]])]] = {
    val pair = Decode.pair(Decode.key[K], Decode.vector(streamEntry[F, V]))
    Decode.shape("stream read map or null") {
      case Frame.Null        => Right(Vector.empty)
      case Frame.Map(rows)   => Decode.each(rows)(pair.tupled)
      case Frame.Array(rows) =>
        Decode.each(rows) {
          case Frame.Array(Vector(n, e)) => pair(n, e)
          case other                     => Left(DecodeError("stream [name, entries] pair", Frame.describe(other)))
        }
    }
  }

  private def autoClaimReply[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, XAutoClaimResult[F, V]] =
    autoClaim(Decode.vector(streamEntry[F, V]), "xautoclaim [cursor, entries, deleted]")(XAutoClaimResult(_, _, _))

  private val autoClaimJustIdReply: Frame => Either[DecodeError, XAutoClaimJustIdResult] =
    autoClaim(Decode.vector(streamId), "xautoclaim justid [cursor, ids, deleted]")(XAutoClaimJustIdResult(_, _, _))

  private def autoClaim[A, R](items: Frame => Either[DecodeError, Vector[A]], label: String)(
    build: (StreamId, Vector[A], Vector[StreamId]) => R
  ): Frame => Either[DecodeError, R] = {
    val deletedIds = Decode.vector(streamId)
    Decode.shape(label) {
      // pre-7.0 omits the deleted-ids element
      case Frame.Array(cursorFrame +: itemsFrame +: rest) if rest.length <= 1 =>
        for {
          cursor  <- streamId(cursorFrame)
          decoded <- items(itemsFrame)
          deleted <- rest.headOption.fold[Either[DecodeError, Vector[StreamId]]](Right(Vector.empty))(deletedIds)
        } yield build(cursor, decoded, deleted)
    }
  }

  private val deletionElement: Frame => Either[DecodeError, StreamEntryDeletion] = Decode.shape("deletion status -1/1/2") {
    case Frame.Integer(-1L) => Right(StreamEntryDeletion.NotFound)
    case Frame.Integer(1L)  => Right(StreamEntryDeletion.Deleted)
    case Frame.Integer(2L)  => Right(StreamEntryDeletion.Retained)
  }

  // XPENDING per-consumer counts come back as bulk-string integers
  private val countText: Frame => Either[DecodeError, Long] = Decode.shape("integer") {
    case Frame.Integer(value)    => Right(value)
    case Frame.BulkString(bytes) => Primitives.decodeLong("integer", Long.MinValue, Long.MaxValue)(bytes)
  }

  private val consumerCount: Frame => Either[DecodeError, (String, Long)] =
    Decode.array2(Decode.utf8String, countText, "[consumer, count] pair")(_ -> _)

  // XPENDING summary: [total, min-id, max-id, [[consumer, count], …]]; an empty group replies [0, nil, nil, nil]
  private val pendingSummaryReply: Frame => Either[DecodeError, PendingSummary] = {
    val consumers = Decode.orEmpty(Decode.vector(consumerCount, "consumer counts array or null"))
    Decode.array4(Decode.long, optionalStreamId, optionalStreamId, consumers, "xpending summary")(PendingSummary(_, _, _, _))
  }

  // XPENDING extended row: [id, consumer, idle-ms, delivery-count]
  private val pendingEntryElement: Frame => Either[DecodeError, PendingEntry] =
    Decode.array4(streamId, Decode.utf8String, Decode.millisDuration, Decode.long, "xpending entry [id, consumer, idle, count]")(
      PendingEntry(_, _, _, _)
    )

}
