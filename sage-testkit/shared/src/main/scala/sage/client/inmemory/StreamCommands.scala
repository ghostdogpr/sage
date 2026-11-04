package sage.client.inmemory

import scala.collection.immutable.{TreeMap, VectorMap}
import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.commands.StreamId
import sage.protocol.Frame

/**
  * Stream commands: entries, ranges, trimming, `XREAD`, consumer groups with their pending entries lists, claiming, and `XINFO`.
  * Approximate trimming (`~`) removes whole blocks of [[StreamCommands.NodeSize]] entries, the default size of a server's stream nodes, so a
  * short stream is not trimmed by it, as on a real server.
  */
private[inmemory] object StreamCommands {
  import Replies.*
  import Value.{Consumer, Group, Pending, Stream}

  // the default stream-node-max-entries of a server, which approximate trimming works in
  val NodeSize: Int = 100

  private val MaxSeq = -1L // the greatest unsigned 64-bit sequence number

  val specs: Map[String, Spec] = Map(
    "XACK"                  -> Spec(-4, xAck),
    "XACKDEL"               -> Spec(-6, xAckDel),
    "XADD"                  -> Spec(-5, xAdd),
    "XAUTOCLAIM"            -> Spec(-6, xAutoClaim),
    "XCFGSET"               -> Spec(-2, xCfgSet),
    "XCLAIM"                -> Spec(-6, xClaim),
    "XDEL"                  -> Spec(-3, xDel),
    "XDELEX"                -> Spec(-4, xDelEx),
    "XGROUP CREATE"         -> Spec(-4, xGroupCreate),
    "XGROUP CREATECONSUMER" -> Spec(4, xGroupCreateConsumer),
    "XGROUP DELCONSUMER"    -> Spec(4, xGroupDelConsumer),
    "XGROUP DESTROY"        -> Spec(3, xGroupDestroy),
    "XGROUP SETID"          -> Spec(-4, xGroupSetId),
    "XINFO CONSUMERS"       -> Spec(3, xInfoConsumers),
    "XINFO GROUPS"          -> Spec(2, xInfoGroups),
    "XINFO STREAM"          -> Spec(-2, xInfoStream),
    "XLEN"                  -> Spec(2, call => int(stream(call, call.key(0)).fold(0L)(_.entries.size.toLong))),
    "XNACK"                 -> Spec(-6, xNack),
    "XPENDING"              -> Spec(-3, xPending),
    "XRANGE"                -> Spec(-4, call => range(call, reverse = false)),
    "XREAD"                 -> Spec(-4, xRead, timeout = Some(blockOption), prepare = Some(resolveLastIds)),
    "XREADGROUP"            -> Spec(-7, xReadGroup, timeout = Some(blockOption)),
    "XREVRANGE"             -> Spec(-4, call => range(call, reverse = true)),
    "XSETID"                -> Spec(-3, xSetId),
    "XTRIM"                 -> Spec(-4, xTrim)
  )

  // --- ids and replies ------------------------------------------------------

  private given Ordering[StreamId] = (a, b) => a.compare(b)

  def idText(id: StreamId): String = s"${java.lang.Long.toUnsignedString(id.ms)}-${java.lang.Long.toUnsignedString(id.seq)}"

  private val invalidId = Errors.err("Invalid stream ID specified as stream command argument")

  private def unsigned(text: String): Option[Long] =
    if (text.isEmpty || !text.forall(_.isDigit)) None
    else
      try Some(java.lang.Long.parseUnsignedLong(text))
      catch { case _: NumberFormatException => None }

  // `ms-seq`, or `ms` alone with the sequence number given by `missingSeq`
  private def parseId(text: String, missingSeq: Long): Option[StreamId] =
    text.indexOf('-') match {
      case -1   => unsigned(text).map(StreamId(_, missingSeq))
      case dash => unsigned(text.take(dash)).zip(unsigned(text.drop(dash + 1))).map(StreamId(_, _))
    }

  private def id(call: Call, index: Int, missingSeq: Long = 0L): StreamId =
    parseId(call.text(index), missingSeq).getOrElse(throw new Reject(invalidId))

  private def next(id: StreamId): Option[StreamId] =
    if (id.seq != MaxSeq) Some(StreamId(id.ms, id.seq + 1)) else Option.when(id.ms != MaxSeq)(StreamId(id.ms + 1, 0L))

  private def previous(id: StreamId): Option[StreamId] =
    if (id.seq != 0L) Some(StreamId(id.ms, id.seq - 1)) else Option.when(id.ms != 0L)(StreamId(id.ms - 1, MaxSeq))

  private def idFrame(id: StreamId): Frame = bulk(idText(id))

  private def fieldsFrame(fields: Vector[(Bytes, Bytes)]): Frame = Frame.Array(fields.flatMap((f, v) => Vector(bulk(f), bulk(v))))

  private def entryFrame(id: StreamId, fields: Vector[(Bytes, Bytes)]): Frame = Frame.Array(Vector(idFrame(id), fieldsFrame(fields)))

  private def entriesFrame(entries: Iterable[(StreamId, Vector[(Bytes, Bytes)])]): Frame =
    Frame.Array(entries.iterator.map(entryFrame(_, _)).toVector)

  private def fieldMap(pairs: (String, Frame)*): Frame = Frame.Map(pairs.toVector.map((name, value) => bulk(name) -> value))

  // --- access ---------------------------------------------------------------

  private def stream(call: Call, key: Key): Option[Stream] = call.typed[Stream](key)

  private def existing(call: Call, key: Key): Stream = stream(call, key).getOrElse(throw new Reject(Errors.noSuchKey))

  private def noGroup(key: Key, group: String, command: String = ""): Reject =
    new Reject(Frame.SimpleError(s"NOGROUP No such key '$key' or consumer group '$group'$command"))

  private def group(call: Call, key: Key, name: String): (Stream, Group) = {
    val s = stream(call, key).getOrElse(throw noGroup(key, name))
    (s, s.groups.getOrElse(name, throw noGroup(key, name)))
  }

  private def save(call: Call, key: Key, s: Stream): Unit = call.store.put(key, s, call.lookup(key).fold(Entry.NoExpiry)(_.expiresAt), call.now)

  private def withGroup(s: Stream, name: String, g: Group): Stream = s.copy(groups = s.groups.updated(name, g))

  private def touchConsumer(g: Group, name: String, now: Long, active: Boolean): Group = {
    val current = g.consumers.getOrElse(name, Consumer(now, -1L))
    g.copy(consumers = g.consumers.updated(name, Consumer(now, if (active) now else current.activeAt)))
  }

  // --- deletion and trimming ------------------------------------------------

  private enum Policy {
    case KeepRef, DelRef, Acked
  }

  private def policyAt(call: Call, index: Int): Option[Policy] =
    call.args.lift(index).map(_.asUtf8String.toUpperCase(java.util.Locale.ROOT)) match {
      case Some("KEEPREF") => Some(Policy.KeepRef)
      case Some("DELREF")  => Some(Policy.DelRef)
      case Some("ACKED")   => Some(Policy.Acked)
      case _               => None
    }

  // an entry counts as acknowledged when every group has read past it and none still has it pending
  private def acknowledged(s: Stream, id: StreamId): Boolean =
    s.groups.valuesIterator.forall(g => g.lastDelivered >= id && !g.pending.contains(id))

  // removes `ids` from the entries, and also from every pending entries list with DELREF
  private def removeEntries(s: Stream, ids: Iterable[StreamId], policy: Policy): Stream = {
    val removed = ids.filter(s.entries.contains)
    if (removed.isEmpty) s
    else {
      val groups =
        if (policy != Policy.DelRef) s.groups
        else s.groups.map((name, g) => name -> g.copy(pending = g.pending -- removed))
      s.copy(entries = s.entries -- removed, maxDeletedId = (s.maxDeletedId +: removed.toVector).max, groups = groups)
    }
  }

  private enum Threshold {
    case MaxLen(count: Long)
    case MinId(id: StreamId)
  }

  final private case class Trim(threshold: Threshold, approximate: Boolean)

  // parses `MAXLEN | MINID [= | ~] threshold [LIMIT count]` at `index` and returns it with the index after it
  private def trimAt(call: Call, index: Int): (Trim, Int) = {
    val kind        = call.keyword(index)
    var i           = index + 1
    val approximate = call.args.lift(i).map(_.asUtf8String) match {
      case Some("~") => i += 1; true
      case Some("=") => i += 1; false
      case _         => false
    }
    if (i >= call.args.length) throw new Reject(Errors.syntax)
    val threshold   = kind match {
      case "MAXLEN" =>
        val n = call.long(i)
        if (n < 0) throw new Reject(Errors.err("The MAXLEN argument must be >= 0."))
        Threshold.MaxLen(n)
      case _        => Threshold.MinId(id(call, i))
    }
    i += 1
    if (i + 1 < call.args.length && call.keyword(i) == "LIMIT") {
      if (!approximate) throw new Reject(Errors.err("syntax error, LIMIT cannot be used without the special ~ option"))
      call.long(i + 1): Unit
      i += 2
    }
    (Trim(threshold, approximate), i)
  }

  private def trim(s: Stream, spec: Trim, policy: Policy): (Stream, Long) = {
    val ids       = s.entries.keysIterator.toVector
    val beyond    = spec.threshold match {
      case Threshold.MaxLen(count) => math.max(0L, ids.length - count).toInt
      case Threshold.MinId(min)    =>
        ids.indexWhere(_ >= min) match {
          case -1 => ids.length
          case n  => n
        }
    }
    val allowed   = if (spec.approximate) beyond / NodeSize * NodeSize else beyond
    // ACKED stops at the first entry that some group still needs
    val removable = if (policy == Policy.Acked) ids.take(allowed).takeWhile(acknowledged(s, _)) else ids.take(allowed)
    (removeEntries(s, removable, policy), removable.length.toLong)
  }

  // --- writes ---------------------------------------------------------------

  private def xAdd(call: Call): Frame = {
    val key        = call.key(0)
    var noMkStream = false
    var policy     = Policy.KeepRef
    var trimming   = Option.empty[Trim]
    var i          = 1
    var parsing    = true
    while (parsing && i < call.args.length)
      call.keyword(i) match {
        case "NOMKSTREAM"                     => noMkStream = true; i += 1
        case "MAXLEN" | "MINID"               =>
          val (t, after) = trimAt(call, i)
          trimming = Some(t)
          i = after
        case _ if policyAt(call, i).isDefined =>
          policy = policyAt(call, i).get
          i += 1
        case _                                => parsing = false
      }
    val pairs      = call.args.length - i - 1
    if (pairs <= 0 || pairs % 2 != 0) throw new Reject(Errors.arity(call.name))
    stream(call, call.key(0)) match {
      case None if noMkStream => Frame.Null
      case current            =>
        val s        = current.getOrElse(Stream.empty)
        val text     = call.text(i)
        val tooSmall = Errors.err("The ID specified in XADD is equal or smaller than the target stream top item")
        val newId    =
          if (text == "*") {
            val ms = if (java.lang.Long.compareUnsigned(call.now, s.lastId.ms) > 0) call.now else s.lastId.ms
            if (ms == s.lastId.ms) next(s.lastId).getOrElse(throw new Reject(tooSmall)) else StreamId(ms, 0L)
          } else if (text.endsWith("-*")) {
            val ms = unsigned(text.dropRight(2)).getOrElse(throw new Reject(invalidId))
            java.lang.Long.compareUnsigned(ms, s.lastId.ms) match {
              case c if c < 0 => throw new Reject(tooSmall)
              case 0          => next(s.lastId).getOrElse(throw new Reject(tooSmall))
              case _          => StreamId(ms, 0L)
            }
          } else {
            val explicit = id(call, i)
            if (explicit == StreamId.Zero) throw new Reject(Errors.err("The ID specified in XADD must be greater than 0-0"))
            if (explicit <= s.lastId) throw new Reject(tooSmall)
            explicit
          }
        val fields   = call.args.drop(i + 1).grouped(2).map(pair => (pair(0), pair(1))).toVector
        val added    = s.copy(entries = s.entries.updated(newId, fields), lastId = newId, entriesAdded = s.entriesAdded + 1)
        save(call, key, trimming.fold(added)(t => trim(added, t, policy)._1))
        idFrame(newId)
    }
  }

  private def xDel(call: Call): Frame = {
    val key = call.key(0)
    val ids = call.args.indices.drop(1).map(id(call, _))
    stream(call, key).fold(int(0L)) { s =>
      val present = ids.distinct.filter(s.entries.contains)
      if (present.nonEmpty) save(call, key, removeEntries(s, present, Policy.KeepRef))
      int(present.length.toLong)
    }
  }

  // `IDS numids id…` starting at `index`
  private def idsBlock(call: Call, index: Int): (Vector[StreamId], Int) = {
    if (index >= call.args.length || call.keyword(index) != "IDS") throw new Reject(Errors.syntax)
    val count = call.long(index + 1)
    if (count <= 0 || index + 2 + count > call.args.length) throw new Reject(Errors.syntax)
    (Vector.tabulate(count.toInt)(n => id(call, index + 2 + n)), index + 2 + count.toInt)
  }

  // one deletion status per id: -1 when the entry does not exist, 1 when it was deleted, 2 when ACKED kept it for a group
  private def deleteWithStatus(s: Stream, ids: Vector[StreamId], policy: Policy): (Stream, Vector[Frame]) =
    ids.foldLeft((s, Vector.empty[Frame])) { case ((current, statuses), id) =>
      if (!current.entries.contains(id)) (current, statuses :+ int(-1L))
      else if (policy == Policy.Acked && !acknowledged(current, id)) (current, statuses :+ int(2L))
      else (removeEntries(current, Vector(id), policy), statuses :+ int(1L))
    }

  private def xDelEx(call: Call): Frame = {
    val key          = call.key(0)
    val policy       = policyAt(call, 1)
    val (ids, after) = idsBlock(call, if (policy.isDefined) 2 else 1)
    if (after != call.args.length) throw new Reject(Errors.syntax)
    stream(call, key).fold(Frame.Array(ids.map(_ => int(-1L)))) { s =>
      val (updated, statuses) = deleteWithStatus(s, ids, policy.getOrElse(Policy.KeepRef))
      save(call, key, updated)
      Frame.Array(statuses)
    }
  }

  private def xAckDel(call: Call): Frame = {
    val key                 = call.key(0)
    val name                = call.text(1)
    val policy              = policyAt(call, 2)
    val (ids, after)        = idsBlock(call, if (policy.isDefined) 3 else 2)
    if (after != call.args.length) throw new Reject(Errors.syntax)
    val (s, g)              = group(call, key, name)
    val acked               = withGroup(s, name, g.copy(pending = g.pending -- ids))
    val (updated, statuses) = deleteWithStatus(acked, ids, policy.getOrElse(Policy.KeepRef))
    save(call, key, updated)
    Frame.Array(statuses)
  }

  private def xTrim(call: Call): Frame = {
    val key          = call.key(0)
    val (spec, next) = trimAt(call, 1)
    val policy       = policyAt(call, next).getOrElse {
      if (next < call.args.length) throw new Reject(Errors.syntax)
      Policy.KeepRef
    }
    stream(call, key).fold(int(0L)) { s =>
      val (updated, removed) = trim(s, spec, policy)
      if (removed > 0) save(call, key, updated)
      int(removed)
    }
  }

  private def xSetId(call: Call): Frame = {
    val key          = call.key(0)
    val s            = existing(call, key)
    val newId        = id(call, 1)
    var entriesAdded = s.entriesAdded
    var maxDeleted   = s.maxDeletedId
    var i            = 2
    while (i + 1 < call.args.length) {
      call.keyword(i) match {
        case "ENTRIESADDED" => entriesAdded = call.long(i + 1)
        case "MAXDELETEDID" => maxDeleted = id(call, i + 1)
        case _              => throw new Reject(Errors.syntax)
      }
      i += 2
    }
    if (i != call.args.length) throw new Reject(Errors.syntax)
    if (s.entries.lastOption.exists((top, _) => newId < top))
      throw new Reject(Errors.err("The ID specified in XSETID is smaller than the target stream top item"))
    if (entriesAdded < s.entries.size)
      throw new Reject(Errors.err("The entries_added specified in XSETID is smaller than the target stream length"))
    save(call, key, s.copy(lastId = newId, entriesAdded = entriesAdded, maxDeletedId = maxDeleted))
    ok
  }

  // XCFGSET configures idempotent XADD, which the in-memory client does not implement, so the settings are accepted and not used
  private def xCfgSet(call: Call): Frame = {
    existing(call, call.key(0)): Unit
    val options = call.args.drop(1)
    if (options.isEmpty || options.length % 2 != 0) throw new Reject(Errors.syntax)
    options.grouped(2).foreach { pair =>
      pair(0).asUtf8String.toUpperCase(java.util.Locale.ROOT) match {
        case "IDMP-DURATION" | "IDMP-MAXSIZE" =>
          if (Numbers.parseLong(pair(1)).forall(_ <= 0)) throw new Reject(Errors.notInteger)
        case _                                => throw new Reject(Errors.syntax)
      }
    }
    ok
  }

  // --- range reads ------------------------------------------------------------

  // converts a range bound to an inclusive id, or None when the bound leaves nothing to return
  private def bound(call: Call, index: Int, lower: Boolean): Option[StreamId] =
    call.text(index) match {
      case "-"                          => Some(StreamId.Zero)
      case "+"                          => Some(StreamId(MaxSeq, MaxSeq))
      case text if text.startsWith("(") =>
        val exclusive = parseId(text.drop(1), if (lower) MaxSeq else 0L).getOrElse(throw new Reject(invalidId))
        if (lower) next(exclusive) else previous(exclusive)
      case text                         => Some(parseId(text, if (lower) 0L else MaxSeq).getOrElse(throw new Reject(invalidId)))
    }

  private def range(call: Call, reverse: Boolean): Frame = {
    val (startIndex, endIndex) = if (reverse) (2, 1) else (1, 2)
    val low                    = bound(call, startIndex, lower = true)
    val high                   = bound(call, endIndex, lower = false)
    val count                  = call.args.length match {
      case 3                               => None
      case 5 if call.keyword(3) == "COUNT" => Some(math.max(0L, call.long(4)))
      case _                               => throw new Reject(Errors.syntax)
    }
    val entries                = stream(call, call.key(0)).fold(Vector.empty[(StreamId, Vector[(Bytes, Bytes)])]) { s =>
      (low, high) match {
        case (Some(lo), Some(hi)) if lo <= hi =>
          val selected = s.entries.range(lo, hi).toVector ++ s.entries.get(hi).map(hi -> _)
          if (reverse) selected.reverse else selected
        case _                                => Vector.empty
      }
    }
    entriesFrame(count.fold(entries)(n => entries.take(math.min(n, Int.MaxValue.toLong).toInt)))
  }

  // --- XREAD and XREADGROUP ---------------------------------------------------

  // the index of STREAMS and the (key, id) pairs after it
  private def streamsBlock(call: Call): (Int, Vector[(Key, String)]) = {
    val at   = call.args.indexWhere(_.asUtf8String.equalsIgnoreCase("STREAMS"))
    if (at < 0) throw new Reject(Errors.syntax)
    val rest = call.args.length - at - 1
    if (rest == 0 || rest % 2 != 0)
      throw new Reject(
        Errors.err(
          s"Unbalanced '${call.name.toLowerCase(java.util.Locale.ROOT)}' list of streams: for each stream key an ID or '${if (call.name == "XREAD") "$"
            else ">"}' must be specified."
        )
      )
    val n = rest / 2
    (at, Vector.tabulate(n)(i => (call.key(at + 1 + i), call.text(at + 1 + n + i))))
  }

  private def blockOption(args: Vector[Bytes]): Option[FiniteDuration] = {
    val at = args.indexWhere(_.asUtf8String.equalsIgnoreCase("BLOCK"))
    if (at < 0 || at + 1 >= args.length) throw new Reject(Errors.syntax)
    Timeouts.millis(args(at + 1))
  }

  // replaces `$` with the stream's current last id, so that a blocked XREAD keeps looking for entries after the id it started from
  private def resolveLastIds(call: Call): Vector[Bytes] = {
    val (at, streams) = streamsBlock(call)
    val n             = streams.length
    call.args.zipWithIndex.map { (arg, index) =>
      if (index > at + n && arg.asUtf8String == "$")
        Bytes.utf8(idText(stream(call, streams(index - at - 1 - n)._1).fold(StreamId.Zero)(_.lastId)))
      else arg
    }
  }

  // COUNT, BLOCK and NOACK before STREAMS; `from` is the first option index
  final private case class ReadOptions(count: Option[Long], block: Boolean, noAck: Boolean)

  private def readOptions(call: Call, from: Int, until: Int, allowNoAck: Boolean): ReadOptions = {
    var count = Option.empty[Long]
    var block = false
    var noAck = false
    var i     = from
    while (i < until)
      call.keyword(i) match {
        case "COUNT" if i + 1 < until => count = Some(call.long(i + 1)).filter(_ > 0); i += 2
        case "BLOCK" if i + 1 < until => block = true; i += 2
        case "NOACK" if allowNoAck    => noAck = true; i += 1
        case _                        => throw new Reject(Errors.syntax)
      }
    ReadOptions(count, block, noAck)
  }

  private def limited[A](items: Vector[A], count: Option[Long]): Vector[A] =
    count.fold(items)(n => items.take(math.min(n, Int.MaxValue.toLong).toInt))

  private def xRead(call: Call): Frame = {
    val (at, streams) = streamsBlock(call)
    val options       = readOptions(call, 0, at, allowNoAck = false)
    val results       = streams.flatMap { (key, text) =>
      val s       = stream(call, key)
      val entries = s.fold(Vector.empty[(StreamId, Vector[(Bytes, Bytes)])]) { st =>
        text match {
          case "+" => st.entries.lastOption.toVector
          case "$" => Vector.empty
          case _   =>
            val after = parseId(text, 0L).getOrElse(throw new Reject(invalidId))
            next(after).fold(Vector.empty)(from => limited(st.entries.rangeFrom(from).toVector, options.count))
        }
      }
      Option.when(entries.nonEmpty)(bulk(key.bytes) -> entriesFrame(entries))
    }
    if (results.nonEmpty) Frame.Map(results)
    else if (options.block) throw Blocked
    else Frame.Null
  }

  private def xReadGroup(call: Call): Frame = {
    if (call.keyword(0) != "GROUP") throw new Reject(Errors.syntax)
    val name          = call.text(1)
    val consumer      = call.text(2)
    val (at, streams) = streamsBlock(call)
    val options       = readOptions(call, 3, at, allowNoAck = true)
    val history       = streams.exists(_._2 != ">")
    // compute every result before writing, so that a read that ends up blocking changes nothing
    val reads         = streams.map { (key, text) =>
      val s = stream(call, key).getOrElse(throw noGroup(key, name, " in XREADGROUP with GROUP option"))
      val g = s.groups.getOrElse(name, throw noGroup(key, name, " in XREADGROUP with GROUP option"))
      if (text == ">") {
        val fresh = next(g.lastDelivered).fold(Vector.empty)(from => limited(s.entries.rangeFrom(from).toVector, options.count))
        (key, s, g, fresh.map((id, fields) => id -> Option(fields)), true)
      } else {
        val after = parseId(text, 0L).getOrElse(throw new Reject(invalidId))
        val owned = g.pending.iterator.filter((id, p) => p.consumer == consumer && id > after).map(_._1).toVector
        (key, s, g, limited(owned, options.count).map(id => id -> s.entries.get(id)), false)
      }
    }
    if (!history && reads.forall(_._4.isEmpty) && options.block) throw Blocked
    val replies       = reads.flatMap { (key, s, g, delivered, fresh) =>
      val pending =
        if (fresh && options.noAck) g.pending
        else
          delivered.foldLeft(g.pending) { case (acc, (id, _)) =>
            acc.updated(id, Pending(consumer, call.now, acc.get(id).fold(1L)(_.deliveries + 1)))
          }
      val updated =
        if (!fresh) g.copy(pending = pending)
        else
          delivered.lastOption.fold(g) { (last, _) =>
            val read = g.entriesRead.map(_ + delivered.length).orElse(Option.when(last == s.lastId)(s.entriesAdded))
            g.copy(lastDelivered = last, entriesRead = read, pending = pending)
          }
      save(call, key, withGroup(s, name, touchConsumer(updated, consumer, call.now, active = delivered.nonEmpty)))
      val entries = Frame.Array(delivered.map((id, fields) => Frame.Array(Vector(idFrame(id), fields.fold(Frame.Null)(fieldsFrame)))))
      Option.when(!fresh || delivered.nonEmpty)(bulk(key.bytes) -> entries)
    }
    if (replies.isEmpty) Frame.Null else Frame.Map(replies)
  }

  // --- groups -----------------------------------------------------------------

  private def startId(call: Call, index: Int, s: Stream): StreamId =
    if (call.text(index) == "$") s.lastId else id(call, index)

  // the number of entries read by a group that starts at `start`, when it can be known
  private def entriesReadAt(s: Stream, start: StreamId, explicit: Option[Long]): Option[Long] =
    explicit.orElse {
      if (start == s.lastId) Some(s.entriesAdded) else Option.when(start == StreamId.Zero && s.maxDeletedId == StreamId.Zero)(0L)
    }

  private def entriesReadOption(call: Call, from: Int): Option[Long] =
    call.args.length - from match {
      case 0                                        => None
      case 2 if call.keyword(from) == "ENTRIESREAD" => Some(call.long(from + 1))
      case _                                        => throw new Reject(Errors.syntax)
    }

  private def xGroupCreate(call: Call): Frame = {
    val key   = call.key(0)
    val name  = call.text(1)
    val mk    = call.args.lift(3).exists(_.asUtf8String.equalsIgnoreCase("MKSTREAM"))
    val s     = stream(call, key).getOrElse {
      if (!mk)
        throw new Reject(
          Errors.err(
            "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
          )
        )
      Stream.empty
    }
    if (s.groups.contains(name)) throw new Reject(Frame.SimpleError("BUSYGROUP Consumer Group name already exists"))
    val start = startId(call, 2, s)
    val read  = entriesReadAt(s, start, entriesReadOption(call, if (mk) 4 else 3))
    save(call, key, withGroup(s, name, Group(start, read, TreeMap.empty, VectorMap.empty)))
    ok
  }

  private def xGroupSetId(call: Call): Frame = {
    val key    = call.key(0)
    val name   = call.text(1)
    val (s, g) = group(call, key, name)
    val start  = startId(call, 2, s)
    save(call, key, withGroup(s, name, g.copy(lastDelivered = start, entriesRead = entriesReadAt(s, start, entriesReadOption(call, 3)))))
    ok
  }

  private def xGroupDestroy(call: Call): Frame = {
    val key = call.key(0)
    val s   = existing(call, key)
    if (!s.groups.contains(call.text(1))) flag(false)
    else {
      save(call, key, s.copy(groups = s.groups - call.text(1)))
      flag(true)
    }
  }

  private def xGroupCreateConsumer(call: Call): Frame = {
    val key    = call.key(0)
    val name   = call.text(1)
    val (s, g) = group(call, key, name)
    if (g.consumers.contains(call.text(2))) flag(false)
    else {
      save(call, key, withGroup(s, name, g.copy(consumers = g.consumers.updated(call.text(2), Consumer(call.now, -1L)))))
      flag(true)
    }
  }

  // deleting a consumer drops its pending entries, and the reply is how many there were
  private def xGroupDelConsumer(call: Call): Frame = {
    val key      = call.key(0)
    val name     = call.text(1)
    val consumer = call.text(2)
    val (s, g)   = group(call, key, name)
    val owned    = g.pending.filter((_, p) => p.consumer == consumer)
    save(call, key, withGroup(s, name, g.copy(pending = g.pending -- owned.keys, consumers = g.consumers - consumer)))
    int(owned.size.toLong)
  }

  private def xAck(call: Call): Frame = {
    val key  = call.key(0)
    val name = call.text(1)
    val ids  = call.args.indices.drop(2).map(id(call, _)).distinct
    stream(call, key).flatMap(s => s.groups.get(name).map(s -> _)).fold(int(0L)) { (s, g) =>
      val acked = ids.filter(g.pending.contains)
      if (acked.nonEmpty) save(call, key, withGroup(s, name, g.copy(pending = g.pending -- acked)))
      int(acked.length.toLong)
    }
  }

  /**
    * XNACK releases pending entries back to the group: they stay pending, with no owning consumer. SILENT leaves the delivery count one
    * lower, FAIL leaves it unchanged, and FATAL sets it to the given RETRYCOUNT or to the largest count, which marks the entry as failed.
    * This command is new in Redis 8.8; these counter rules follow its documentation.
    */
  private def xNack(call: Call): Frame = {
    val key          = call.key(0)
    val name         = call.text(1)
    val mode         = call.keyword(2)
    if (!Set("SILENT", "FAIL", "FATAL").contains(mode)) throw new Reject(Errors.syntax)
    val (ids, after) = idsBlock(call, 3)
    var retryCount   = Option.empty[Long]
    var force        = false
    var i            = after
    while (i < call.args.length)
      call.keyword(i) match {
        case "RETRYCOUNT" if i + 1 < call.args.length => retryCount = Some(call.long(i + 1)); i += 2
        case "FORCE"                                  => force = true; i += 1
        case _                                        => throw new Reject(Errors.syntax)
      }
    val (s, g)       = group(call, key, name)
    var released     = 0L
    val pending      = ids.foldLeft(g.pending) { (acc, id) =>
      acc.get(id).orElse(Option.when(force && s.entries.contains(id))(Pending("", call.now, 0L))) match {
        case None    => acc
        case Some(p) =>
          released += 1
          val deliveries = retryCount.getOrElse(mode match {
            case "SILENT" => math.max(0L, p.deliveries - 1)
            case "FATAL"  => Long.MaxValue
            case _        => p.deliveries
          })
          acc.updated(id, Pending("", p.deliveredAt, deliveries))
      }
    }
    save(call, key, withGroup(s, name, g.copy(pending = pending)))
    int(released)
  }

  // --- pending and claiming ---------------------------------------------------

  private def xPending(call: Call): Frame = {
    val key    = call.key(0)
    val name   = call.text(1)
    val (_, g) = group(call, key, name)
    if (call.args.length == 2) {
      if (g.pending.isEmpty) Frame.Array(Vector(int(0L), Frame.Null, Frame.Null, Frame.Null))
      else {
        val counts = g.pending.valuesIterator.toVector.groupBy(_.consumer).toVector.sortBy(_._1)
        Frame.Array(
          Vector(
            int(g.pending.size.toLong),
            idFrame(g.pending.firstKey),
            idFrame(g.pending.lastKey),
            Frame.Array(counts.map((consumer, entries) => Frame.Array(Vector(bulk(consumer), bulk(entries.length.toString)))))
          )
        )
      }
    } else {
      var i        = 2
      val minIdle  =
        if (call.keyword(i) != "IDLE") 0L
        else {
          i += 2
          call.long(i - 1)
        }
      if (i + 3 > call.args.length || i + 4 < call.args.length) throw new Reject(Errors.syntax)
      val low      = bound(call, i, lower = true)
      val high     = bound(call, i + 1, lower = false)
      val count    = call.long(i + 2)
      val consumer = call.args.lift(i + 3).map(_.asUtf8String)
      val rows     = (low, high) match {
        case (Some(lo), Some(hi)) if lo <= hi && count > 0 =>
          g.pending.iterator
            .filter((id, p) => id >= lo && id <= hi && consumer.forall(_ == p.consumer) && call.now - p.deliveredAt >= minIdle)
            .take(math.min(count, Int.MaxValue.toLong).toInt)
            .toVector
        case _                                             => Vector.empty
      }
      Frame.Array(rows.map((id, p) => Frame.Array(Vector(idFrame(id), bulk(p.consumer), int(call.now - p.deliveredAt), int(p.deliveries)))))
    }
  }

  private def xClaim(call: Call): Frame = {
    val key           = call.key(0)
    val name          = call.text(1)
    val consumer      = call.text(2)
    val minIdle       = call.long(3)
    val firstOpt      = call.args.indexWhere(
      arg => Set("IDLE", "TIME", "RETRYCOUNT", "FORCE", "JUSTID", "LASTID").contains(arg.asUtf8String.toUpperCase(java.util.Locale.ROOT)),
      4
    )
    val idsEnd        = if (firstOpt < 0) call.args.length else firstOpt
    val ids           = (4 until idsEnd).map(id(call, _)).toVector
    var deliveredAt   = call.now
    var retryCount    = Option.empty[Long]
    var force         = false
    var justId        = false
    var lastId        = Option.empty[StreamId]
    var i             = idsEnd
    while (i < call.args.length)
      call.keyword(i) match {
        case "IDLE" if i + 1 < call.args.length       => deliveredAt = call.now - call.long(i + 1); i += 2
        case "TIME" if i + 1 < call.args.length       => deliveredAt = call.long(i + 1); i += 2
        case "RETRYCOUNT" if i + 1 < call.args.length => retryCount = Some(call.long(i + 1)); i += 2
        case "LASTID" if i + 1 < call.args.length     => lastId = Some(id(call, i + 1)); i += 2
        case "FORCE"                                  => force = true; i += 1
        case "JUSTID"                                 => justId = true; i += 1
        case _                                        => throw new Reject(Errors.syntax)
      }
    val (s, g)        = group(call, key, name)
    var pending       = g.pending
    val claimed       = ids.flatMap { id =>
      pending.get(id).orElse(Option.when(force && s.entries.contains(id))(Pending(consumer, call.now, 0L))) match {
        case Some(p) if call.now - p.deliveredAt >= minIdle || !g.pending.contains(id) =>
          if (!s.entries.contains(id)) {
            // an entry deleted from the stream leaves the pending list instead of being claimed
            pending = pending - id
            None
          } else {
            val deliveries = retryCount.getOrElse(if (justId) p.deliveries else p.deliveries + 1)
            pending = pending.updated(id, Pending(consumer, deliveredAt, deliveries))
            Some(id)
          }
        case _                                                                         => None
      }
    }
    val lastDelivered = lastId.filter(_ > g.lastDelivered).getOrElse(g.lastDelivered)
    val updated       = touchConsumer(g.copy(pending = pending, lastDelivered = lastDelivered), consumer, call.now, active = claimed.nonEmpty)
    save(call, key, withGroup(s, name, updated))
    if (justId) Frame.Array(claimed.map(idFrame)) else entriesFrame(claimed.map(id => id -> s.entries(id)))
  }

  private def xAutoClaim(call: Call): Frame = {
    val key        = call.key(0)
    val name       = call.text(1)
    val consumer   = call.text(2)
    val minIdle    = call.long(3)
    val start      = parseId(call.text(4), 0L).getOrElse(throw new Reject(invalidId))
    var count      = 100L
    var justId     = false
    var i          = 5
    while (i < call.args.length)
      call.keyword(i) match {
        case "COUNT" if i + 1 < call.args.length =>
          count = call.long(i + 1)
          if (count < 1) throw new Reject(Errors.err("COUNT must be > 0"))
          i += 2
        case "JUSTID"                            => justId = true; i += 1
        case _                                   => throw new Reject(Errors.syntax)
      }
    val (s, g)     = group(call, key, name)
    var pending    = g.pending
    var claimed    = Vector.empty[StreamId]
    var deleted    = Vector.empty[StreamId]
    val candidates = g.pending.iteratorFrom(start)
    var cursor     = StreamId.Zero
    var scanning   = true
    while (scanning && candidates.hasNext) {
      val (id, p) = candidates.next()
      if (claimed.length >= count) {
        cursor = id
        scanning = false
      } else if (call.now - p.deliveredAt >= minIdle) {
        if (!s.entries.contains(id)) {
          pending = pending - id
          deleted :+= id
        } else {
          pending = pending.updated(id, Pending(consumer, call.now, if (justId) p.deliveries else p.deliveries + 1))
          claimed :+= id
        }
      }
    }
    save(call, key, withGroup(s, name, touchConsumer(g.copy(pending = pending), consumer, call.now, active = claimed.nonEmpty)))
    val items      = if (justId) Frame.Array(claimed.map(idFrame)) else entriesFrame(claimed.map(id => id -> s.entries(id)))
    Frame.Array(Vector(idFrame(cursor), items, Frame.Array(deleted.map(idFrame))))
  }

  // --- XINFO ------------------------------------------------------------------

  private def lag(s: Stream, g: Group): Frame = g.entriesRead.fold(Frame.Null)(read => int(math.max(0L, s.entriesAdded - read)))

  private def summary(s: Stream): Vector[(String, Frame)] =
    Vector(
      "length"                  -> int(s.entries.size.toLong),
      "radix-tree-keys"         -> int(math.max(1L, (s.entries.size + NodeSize - 1) / NodeSize).toLong),
      "radix-tree-nodes"        -> int(math.max(1L, (s.entries.size + NodeSize - 1) / NodeSize + 1).toLong),
      "last-generated-id"       -> idFrame(s.lastId),
      "max-deleted-entry-id"    -> idFrame(s.maxDeletedId),
      "entries-added"           -> int(s.entriesAdded),
      "recorded-first-entry-id" -> idFrame(s.entries.headOption.fold(StreamId.Zero)(_._1))
    )

  private def xInfoStream(call: Call): Frame = {
    val s = existing(call, call.key(0))
    call.args.length match {
      case 1                              =>
        fieldMap(
          (summary(s) ++ Vector(
            "groups"      -> int(s.groups.size.toLong),
            "first-entry" -> s.entries.headOption.fold(Frame.Null)(entryFrame(_, _)),
            "last-entry"  -> s.entries.lastOption.fold(Frame.Null)(entryFrame(_, _))
          ))*
        )
      case n if call.keyword(1) == "FULL" =>
        val count                                  = n match {
          case 2                               => 10L
          case 4 if call.keyword(2) == "COUNT" => call.long(3)
          case _                               => throw new Reject(Errors.syntax)
        }
        def take[A](items: Iterable[A]): Vector[A] = if (count <= 0) items.toVector else items.iterator.take(count.toInt).toVector
        val groups                                 = s.groups.toVector.map { (name, g) =>
          fieldMap(
            "name"              -> bulk(name),
            "last-delivered-id" -> idFrame(g.lastDelivered),
            "entries-read"      -> g.entriesRead.fold(Frame.Null)(int),
            "lag"               -> lag(s, g),
            "pel-count"         -> int(g.pending.size.toLong),
            "pending"           -> Frame.Array(
              take(g.pending).map((id, p) => Frame.Array(Vector(idFrame(id), bulk(p.consumer), int(p.deliveredAt), int(p.deliveries))))
            ),
            "consumers"         -> Frame.Array(g.consumers.toVector.map { (consumer, c) =>
              val owned        = g.pending.filter((_, p) => p.consumer == consumer)
              fieldMap(
                "name"        -> bulk(consumer),
                "seen-time"   -> int(c.seenAt),
                "active-time" -> int(c.activeAt),
                "pel-count"   -> int(owned.size.toLong),
                "pending"     -> Frame.Array(take(owned).map((id, p) => Frame.Array(Vector(idFrame(id), int(p.deliveredAt), int(p.deliveries)))))
              )
            })
          )
        }
        fieldMap((summary(s) ++ Vector("entries" -> entriesFrame(take(s.entries)), "groups" -> Frame.Array(groups)))*)
      case _                              => throw new Reject(Errors.syntax)
    }
  }

  private def xInfoGroups(call: Call): Frame = {
    val s = existing(call, call.key(0))
    Frame.Array(s.groups.toVector.map { (name, g) =>
      fieldMap(
        "name"              -> bulk(name),
        "consumers"         -> int(g.consumers.size.toLong),
        "pending"           -> int(g.pending.size.toLong),
        "last-delivered-id" -> idFrame(g.lastDelivered),
        "entries-read"      -> g.entriesRead.fold(Frame.Null)(int),
        "lag"               -> lag(s, g)
      )
    })
  }

  private def xInfoConsumers(call: Call): Frame = {
    val key    = call.key(0)
    val (_, g) = group(call, key, call.text(1))
    Frame.Array(g.consumers.toVector.map { (name, c) =>
      fieldMap(
        "name"     -> bulk(name),
        "pending"  -> int(g.pending.count((_, p) => p.consumer == name).toLong),
        "idle"     -> int(call.now - c.seenAt),
        "inactive" -> int(if (c.activeAt < 0) -1L else call.now - c.activeAt)
      )
    })
  }
}
