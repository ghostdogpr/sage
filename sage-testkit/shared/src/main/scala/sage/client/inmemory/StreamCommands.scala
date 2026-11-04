package sage.client.inmemory

import scala.collection.immutable.{TreeMap, VectorMap}
import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.commands.StreamId
import sage.protocol.Frame

/**
  * Stream commands: entries, ranges, trimming, `XREAD`, consumer groups with their pending entries lists, claiming, and `XINFO`.
  * Approximate trimming (`~`) trims exactly, removing at most `LIMIT` entries when it is given.
  */
private[inmemory] object StreamCommands {
  import Replies.*
  import Value.{Consumer, Group, Pending, Stream}

  private val MaxSeq = -1L // the greatest unsigned 64-bit sequence number

  val specs: Map[String, Spec] = Map(
    "XACK"                  -> Spec(-4, xAck),
    "XADD"                  -> Spec(-5, xAdd),
    "XAUTOCLAIM"            -> Spec(-6, xAutoClaim),
    "XCLAIM"                -> Spec(-6, xClaim),
    "XDEL"                  -> Spec(-3, xDel),
    "XGROUP CREATE"         -> Spec(-4, groupStart(_, create = true)),
    "XGROUP CREATECONSUMER" -> Spec(4, xGroupCreateConsumer),
    "XGROUP DELCONSUMER"    -> Spec(4, xGroupDelConsumer),
    "XGROUP DESTROY"        -> Spec(3, xGroupDestroy),
    "XGROUP SETID"          -> Spec(-4, groupStart(_, create = false)),
    "XINFO CONSUMERS"       -> Spec(3, xInfoConsumers),
    "XINFO GROUPS"          -> Spec(2, xInfoGroups),
    "XINFO STREAM"          -> Spec(-2, xInfoStream),
    "XLEN"                  -> Spec(2, call => int(stream(call, call.key(0)).fold(0L)(_.entries.size.toLong))),
    "XPENDING"              -> Spec(-3, xPending),
    "XRANGE"                -> Spec(-4, call => range(call, reverse = false)),
    "XREAD"                 -> Spec(-4, call => xRead(call, readArgs(call)), Some(call => waiting(resolveLast(call, readArgs(call)), xRead))),
    "XREADGROUP"            -> Spec(-7, call => xReadGroup(call, readArgs(call)), Some(joinGroup)),
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

  private def noGroup(key: Key, group: String): Reject = new Reject(Frame.SimpleError(s"NOGROUP No such key '$key' or consumer group '$group'"))

  private def group(call: Call, key: Key, name: String): (Stream, Group) = {
    val s = stream(call, key).getOrElse(throw noGroup(key, name))
    (s, s.groups.getOrElse(name, throw noGroup(key, name)))
  }

  private val keyRequired: Frame.SimpleError = Errors.err("The XGROUP subcommand requires the key to exist")

  // the stream at a group command's first argument and the group that its second argument names, which `putGroup` writes back
  private def group(call: Call): (Stream, Group) = group(call, call.key(0), call.text(1))

  // XGROUP and XINFO CONSUMERS tell a missing key, which fails with `noKey`, from a missing group
  private def groupOf(call: Call, noKey: => Frame.SimpleError): (Stream, Group) = {
    val (key, name) = (call.key(0), call.text(1))
    val s           = stream(call, key).getOrElse(throw new Reject(noKey))
    (s, s.groups.getOrElse(name, throw new Reject(Frame.SimpleError(s"NOGROUP No such consumer group '$name' for key name '$key'"))))
  }

  private def putGroup(call: Call, s: Stream, g: Group, watched: Boolean = false): Unit =
    call.put(call.key(0), withGroup(s, call.text(1), g), watched = watched)

  private def withGroup(s: Stream, name: String, g: Group): Stream = s.copy(groups = s.groups.updated(name, g))

  private def touchConsumer(g: Group, name: String, now: Long, active: Boolean): Group = {
    val current = g.consumers.get(name).flatMap(_.activeAt)
    g.copy(consumers = g.consumers.updated(name, Consumer(now, if (active) Some(now) else current)))
  }

  // --- trimming ---------------------------------------------------------------

  private enum Threshold {
    case MaxLen(count: Long)
    case MinId(id: StreamId)
  }

  // `limit` is the most entries that approximate trimming (`~`) may remove, or None without a limit
  final private case class Trim(threshold: Threshold, limit: Option[Long])

  /**
    * Reads `[MAXLEN | MINID [= | ~] threshold] [LIMIT count]` in any order from argument 1. `other` consumes another word by returning
    * true, or ends the options at it by returning false.
    */
  private inline def trimOptions(call: Call, inline required: Boolean)(inline other: String => Boolean): Option[Trim] = {
    var threshold   = Option.empty[Threshold]
    var approximate = false
    var limit       = Option.empty[Long]
    var parsing     = true
    call.seek(1)
    while (parsing && call.hasNext) {
      val more = call.remaining > 1
      call.keyword(call.position) match {
        case kind @ ("MAXLEN" | "MINID") if more && threshold.isEmpty =>
          call.next(): Unit
          approximate = call.remaining > 1 && call.nextIs("~")
          if (!approximate && call.remaining > 1) call.nextIs("="): Unit
          threshold = Some(if (kind == "MINID") Threshold.MinId(id(call, call.nextIndex())) else Threshold.MaxLen(atLeastZero(call)))
        case "LIMIT" if more                                          => call.next(): Unit; limit = Some(atLeastZero(call))
        case policy @ ("KEEPREF" | "DELREF" | "ACKED")                => call.unsupported(policy)
        case word if other(word)                                      => call.next(): Unit
        case _                                                        => parsing = false
      }
    }
    if (threshold.isEmpty && (required || limit.isDefined) || limit.isDefined && !approximate) throw new Reject(Errors.syntax)
    // LIMIT 0 removes without a limit
    threshold.map(Trim(_, limit.filter(_ > 0)))
  }

  private def atLeastZero(call: Call): Long = call.nextBounded(Errors.outOfRange)(_ >= 0)

  private def trim(s: Stream, spec: Trim): (Stream, Long) = {
    val due     = spec.threshold match {
      case Threshold.MaxLen(count) => s.entries.keysIterator.take(math.max(0L, s.entries.size - count).toInt)
      case Threshold.MinId(min)    => s.entries.keysIterator.takeWhile(_ < min)
    }
    val removed = spec.limit.fold(due)(n => due.take(Numbers.size(n))).toVector
    (s.copy(entries = s.entries -- removed), removed.length.toLong)
  }

  // --- writes ---------------------------------------------------------------

  private type IdRule = StreamId => Option[StreamId]

  // each form of XADD's id gives the new id after the stream's last id
  private def newId(call: Call, text: String): IdRule = text match {
    case "*"                      => last => if (java.lang.Long.compareUnsigned(call.now, last.ms) > 0) Some(StreamId(call.now, 0L)) else next(last)
    case _ if text.endsWith("-*") =>
      val ms = unsigned(text.dropRight(2)).getOrElse(throw new Reject(invalidId))
      last => if (ms != last.ms) Some(StreamId(ms, 0L)) else Option.when(last.seq != MaxSeq)(StreamId(ms, last.seq + 1))
    case _                        =>
      val explicit = parseId(text, 0L).getOrElse(throw new Reject(invalidId))
      _ => Some(explicit)
  }

  private def xAdd(call: Call): Frame = {
    val key              = call.key(0)
    var noMkStream       = false
    var rule             = Option.empty[IdRule]
    val trimming         = trimOptions(call, required = false) {
      case "NOMKSTREAM" => noMkStream = true; true
      case _            => rule = Some(newId(call, call.text(call.position))); false
    }
    val i                = call.position
    val pairs            = call.args.length - i - 1
    val generate: IdRule = rule match {
      case Some(generate) if pairs > 0 && pairs % 2 == 0 => generate
      case _ => throw new Reject(Errors.arity(call.name))
    }
    if (parseId(call.text(i), 0L).contains(StreamId.Zero)) throw new Reject(Errors.err("The ID specified in XADD must be greater than 0-0"))
    stream(call, key) match {
      case None if noMkStream => Frame.Null
      case current            =>
        val s      = current.getOrElse(Stream.empty)
        if (s.lastId == StreamId(MaxSeq, MaxSeq))
          throw new Reject(Errors.err("The stream has exhausted the last possible ID, unable to add more items"))
        val newId  = generate(s.lastId)
          .filter(_ > s.lastId)
          .getOrElse(throw new Reject(Errors.err("The ID specified in XADD is equal or smaller than the target stream top item")))
        val fields = call.args.drop(i + 1).grouped(2).map(pair => (pair(0), pair(1))).toVector
        val added  = s.copy(entries = s.entries.updated(newId, fields), lastId = newId, entriesAdded = s.entriesAdded + 1)
        call.put(key, trimming.fold(added)(trim(added, _)._1))
        idFrame(newId)
    }
  }

  // XDEL raises the greatest deleted id, which trimming leaves as it was, as on a server
  private def xDel(call: Call): Frame = {
    val key = call.key(0)
    stream(call, key).fold(int(0L)) { s =>
      val present = call.args.indices.drop(1).map(id(call, _)).distinct.filter(s.entries.contains)
      if (present.nonEmpty) call.put(key, s.copy(entries = s.entries -- present, maxDeletedId = (s.maxDeletedId +: present).max))
      int(present.length.toLong)
    }
  }

  private def xTrim(call: Call): Frame = {
    val key      = call.key(0)
    val trimming = trimOptions(call, required = true)(_ => throw new Reject(Errors.syntax))
    stream(call, key).fold(int(0L)) { s =>
      val (updated, removed) = trimming.fold((s, 0L))(trim(s, _))
      if (removed > 0) call.put(key, updated)
      int(removed)
    }
  }

  private def xSetId(call: Call): Frame = {
    call.options(2) {
      case option @ ("ENTRIESADDED" | "MAXDELETEDID") => call.unsupported(option)
      case _                                          => throw new Reject(Errors.syntax)
    }
    val newId = id(call, 1)
    val s     = existing(call, call.key(0))
    if (newId < s.maxDeletedId || s.entries.lastOption.exists((top, _) => newId < top))
      throw new Reject(Errors.err("The ID specified in XSETID is smaller than the target stream top item or the greatest deleted id"))
    call.put(call.key(0), s.copy(lastId = newId), watched = false)
    ok
  }

  // --- range reads ------------------------------------------------------------

  // converts a range bound to an inclusive id; a missing sequence number is 0 in a start and the largest one in an end, also after `(`
  private def bound(call: Call, index: Int, lower: Boolean): StreamId = {
    val missingSeq = if (lower) 0L else MaxSeq
    call.text(index) match {
      case "-"                          => StreamId.Zero
      case "+"                          => StreamId(MaxSeq, MaxSeq)
      case text if text.startsWith("(") =>
        val exclusive = parseId(text.drop(1), missingSeq).getOrElse(throw new Reject(invalidId))
        (if (lower) next(exclusive) else previous(exclusive)).getOrElse(throw new Reject(invalidId))
      case text                         => parseId(text, missingSeq).getOrElse(throw new Reject(invalidId))
    }
  }

  private def range(call: Call, reverse: Boolean): Frame = {
    val (startIndex, endIndex) = if (reverse) (2, 1) else (1, 2)
    val low                    = bound(call, startIndex, lower = true)
    val high                   = bound(call, endIndex, lower = false)
    var count                  = Option.empty[Long]
    call.options(3) {
      case "COUNT" => count = Some(math.max(0L, call.nextLong()))
      case _       => throw new Reject(Errors.syntax)
    }
    stream(call, call.key(0)) match {
      case Some(_) if count.contains(0L) => Frame.Null
      case found                         =>
        val selected =
          found.filter(_ => low <= high).fold(Vector.empty)(s => s.entries.range(low, high).toVector ++ s.entries.get(high).map(high -> _))
        entriesFrame(limited(if (reverse) selected.reverse else selected, count))
    }
  }

  // --- XREAD and XREADGROUP ---------------------------------------------------

  /**
    * The arguments of XREAD and XREADGROUP: the options before STREAMS, which a server reads in one pass in any order, the (key, id) pairs
    * after it, which end the arguments. `block` holds the timeout of BLOCK when it was given.
    */
  final private case class Read(
    group: Option[(String, String)],
    count: Option[Long],
    block: Option[Option[FiniteDuration]],
    noAck: Boolean,
    streams: Vector[(Key, String)]
  )

  private def readArgs(call: Call): Read = {
    val grouped   = call.name.equalsIgnoreCase("XREADGROUP")
    var group     = Option.empty[(String, String)]
    var count     = Option.empty[Long]
    var block     = Option.empty[Option[FiniteDuration]]
    var noAck     = false
    var streamsAt = -1
    call.seek(0)
    while (streamsAt < 0)
      call.nextKeyword() match {
        case "COUNT"                                   => count = Some(call.nextLong()).filter(_ > 0)
        case "BLOCK"                                   => block = Some(Timeouts.millis(call.next(), call.now))
        case "STREAMS" if call.hasNext                 => streamsAt = call.position
        case "GROUP" if grouped && call.remaining >= 2 => group = Some((call.next().asUtf8String, call.next().asUtf8String))
        case "NOACK" if grouped                        => noAck = true
        case "CLAIM" if grouped                        => call.unsupported("CLAIM")
        case _                                         => throw new Reject(Errors.syntax)
      }
    val rest      = call.args.length - streamsAt
    if (rest % 2 != 0) throw new Reject(Errors.err("Unbalanced list of streams: each stream key needs an ID"))
    val n = rest / 2
    Read(group, count, block, noAck, Vector.tabulate(n)(i => (call.key(streamsAt + i), call.text(streamsAt + n + i))))
  }

  private def waiting(options: Read, read: (Call, Read) => Frame): Wait =
    Wait(options.block.getOrElse(throw new Reject(Errors.syntax)), read(_, options))

  // replaces `$` with the stream's current last id, so that a blocked XREAD keeps looking for entries after the id it started from; for a key
  // of another type, the first attempt replies WRONGTYPE whatever the id
  private def resolveLast(call: Call, options: Read): Read =
    options.copy(streams = options.streams.map { (key, text) =>
      key -> (if (text != "$") text else idText(call.lookup(key).map(_.value).collect { case s: Stream => s.lastId }.getOrElse(StreamId.Zero)))
    })

  // a server adds the consumer, or updates its seen time, before the command blocks
  private def joinGroup(call: Call): Wait = {
    val options = readArgs(call)
    val wait    = waiting(options, xReadGroup(_, _, retry = true))
    xReadGroup(call, options, join = true): Unit
    wait
  }

  private def limited[A](items: Vector[A], count: Option[Long]): Vector[A] =
    count.fold(items)(n => items.take(Numbers.size(n)))

  private def xRead(call: Call, options: Read): Frame = {
    val results = options.streams.flatMap { (key, text) =>
      val s       = stream(call, key)
      val entries = text match {
        case "+" => s.flatMap(_.entries.lastOption).toVector
        case "$" => Vector.empty
        case _   =>
          val after = parseId(text, 0L).getOrElse(throw new Reject(invalidId))
          s.fold(Vector.empty)(st => next(after).fold(Vector.empty)(from => limited(st.entries.rangeFrom(from).toVector, options.count)))
      }
      Option.when(entries.nonEmpty)(bulk(key.bytes) -> entriesFrame(entries))
    }
    if (results.nonEmpty) Frame.Map(results) else Frame.Null
  }

  private def xReadGroup(call: Call, options: Read, join: Boolean = false, retry: Boolean = false): Frame = {
    val (name, consumer) = options.group.getOrElse(throw new Reject(Errors.syntax))
    // each stream is read as the earlier ones in the command left it, and nothing is written before every stream passed its checks, so that
    // a retry that finds nothing changes nothing
    var states           = VectorMap.empty[Key, (Stream, Group)]
    val replies          = options.streams.flatMap { (key, text) =>
      val (s, g)    = states.getOrElse(key, group(call, key, name))
      if (text == "$" || text == "+") throw new Reject(Errors.err(s"The $text ID is meaningless in the context of XREADGROUP"))
      val fresh     = text == ">"
      val found     =
        if (fresh)
          next(g.lastDelivered)
            .fold(Vector.empty)(from => limited(s.entries.rangeFrom(from).toVector, options.count))
            .map((id, fields) => id -> Option(fields))
        else {
          val after = parseId(text, 0L).getOrElse(throw new Reject(invalidId))
          val owned = g.pending.iterator.filter((id, p) => p.consumer == consumer && id > after).map(_._1).toVector
          limited(owned, options.count).map(id => id -> s.entries.get(id))
        }
      val delivered = if (join) Vector.empty else found
      val pending   =
        if (fresh && options.noAck) g.pending
        else
          delivered.foldLeft(g.pending) {
            // a `>` read delivers afresh, and a history read leaves an entry deleted from the stream as it was
            case (acc, (id, Some(_))) => acc.updated(id, Pending(consumer, call.now, if (fresh) 1L else acc.get(id).fold(1L)(_.deliveries + 1)))
            case (acc, (_, None))     => acc
          }
      val updated   = g.copy(pending = pending, lastDelivered = if (fresh) delivered.lastOption.fold(g.lastDelivered)(_._1) else g.lastDelivered)
      states = states.updated(key, (s, touchConsumer(updated, consumer, call.now, active = fresh && !options.noAck && delivered.nonEmpty)))
      val entries   = Frame.Array(delivered.map((id, fields) => Frame.Array(Vector(idFrame(id), fields.fold(Frame.Null)(fieldsFrame)))))
      Option.when(!fresh || delivered.nonEmpty)(bulk(key.bytes) -> entries)
    }
    if (!retry || replies.nonEmpty) states.foreach { case (key, (s, g)) =>
      call.put(key, withGroup(s, name, g), watched = false)
    }
    if (replies.isEmpty) Frame.Null else Frame.Map(replies)
  }

  // --- groups -----------------------------------------------------------------

  // CREATE and SETID, where only CREATE accepts MKSTREAM
  private def groupStart(call: Call, create: Boolean): Frame = {
    var mk     = false
    call.options(3) {
      case "MKSTREAM" if create && !mk => mk = true
      case "ENTRIESREAD"               => call.unsupported("ENTRIESREAD")
      case _                           => throw new Reject(Errors.syntax)
    }
    val (s, g) =
      if (!create) groupOf(call, keyRequired)
      else
        (
          stream(call, call.key(0)).getOrElse(if (mk) Stream.empty else throw new Reject(keyRequired)),
          Group(StreamId.Zero, TreeMap.empty, TreeMap.empty)
        )
    val start  = if (call.text(2) == "$") s.lastId else id(call, 2)
    if (create && s.groups.contains(call.text(1))) throw new Reject(Frame.SimpleError("BUSYGROUP Consumer Group name already exists"))
    // WATCH sees MKSTREAM create the stream
    putGroup(call, s, g.copy(lastDelivered = start), watched = call.lookup(call.key(0)).isEmpty)
    ok
  }

  private def xGroupDestroy(call: Call): Frame = {
    val key = call.key(0)
    val s   = stream(call, key).getOrElse(throw new Reject(keyRequired))
    if (!s.groups.contains(call.text(1))) flag(false)
    else {
      call.put(key, s.copy(groups = s.groups - call.text(1)), watched = false)
      flag(true)
    }
  }

  private def xGroupCreateConsumer(call: Call): Frame = {
    val (s, g) = groupOf(call, keyRequired)
    if (g.consumers.contains(call.text(2))) flag(false)
    else {
      putGroup(call, s, g.copy(consumers = g.consumers.updated(call.text(2), Consumer(call.now, None))))
      flag(true)
    }
  }

  // deleting a consumer drops its pending entries, and the reply is how many there were
  private def xGroupDelConsumer(call: Call): Frame = {
    val consumer = call.text(2)
    val (s, g)   = groupOf(call, keyRequired)
    val owned    = g.pending.filter((_, p) => p.consumer == consumer)
    putGroup(call, s, g.copy(pending = g.pending -- owned.keys, consumers = g.consumers - consumer))
    int(owned.size.toLong)
  }

  private def xAck(call: Call): Frame =
    stream(call, call.key(0)).flatMap(s => s.groups.get(call.text(1)).map(s -> _)).fold(int(0L)) { (s, g) =>
      val acked = call.args.indices.drop(2).map(id(call, _)).distinct.filter(g.pending.contains)
      if (acked.nonEmpty) putGroup(call, s, g.copy(pending = g.pending -- acked))
      int(acked.length.toLong)
    }

  // --- pending and claiming ---------------------------------------------------

  private def xPending(call: Call): Frame =
    if (call.args.length == 2) {
      val (_, g) = group(call)
      if (g.pending.isEmpty) Frame.Array(Vector(int(0L), Frame.Null, Frame.Null, Frame.Null))
      else {
        val counts = g.pending.valuesIterator.map(_.consumer).toVector.groupBy(identity).toVector.sortBy(_._1)
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
      // `[IDLE min-idle-time] start end count [consumer]`
      if (call.args.length < 5 || call.args.length > 8) throw new Reject(Errors.syntax)
      call.seek(2)
      val minIdle  = if (call.nextIs("IDLE")) call.nextLong() else 0L
      val i        = call.position
      if (i + 3 > call.args.length) throw new Reject(Errors.syntax)
      val count    = call.long(i + 2)
      val low      = bound(call, i, lower = true)
      val high     = bound(call, i + 1, lower = false)
      val consumer = call.args.lift(i + 3).map(_.asUtf8String)
      val (_, g)   = group(call)
      val rows     =
        if (low > high || count <= 0) Vector.empty
        else
          g.pending.iterator
            .filter((id, p) => id >= low && id <= high && consumer.forall(_ == p.consumer) && p.idleFor(minIdle, call.now))
            .take(Numbers.size(count))
            .toVector
      Frame.Array(rows.map { (id, p) =>
        Frame.Array(Vector(idFrame(id), bulk(p.consumer), int(math.max(0L, call.now - p.deliveredAt)), int(p.deliveries)))
      })
    }

  private def xClaim(call: Call): Frame = {
    val consumer    = call.text(2)
    val (s, g)      = group(call)
    val minIdle     = call.long(3)
    // as on a server, the ids end at the first argument that is not one, and the options follow
    val ids         = call.args.iterator.drop(4).map(arg => parseId(arg.asUtf8String, 0L)).takeWhile(_.isDefined).flatten.toVector
    var deliveredAt = call.now
    var retryCount  = Option.empty[Long]
    var justId      = false
    call.options(4 + ids.length) {
      case "IDLE"                        => deliveredAt = call.now - call.nextLong()
      case "TIME"                        => deliveredAt = call.nextLong()
      // a negative count means none, as on a server
      case "RETRYCOUNT"                  => retryCount = Some(call.nextLong()).filter(_ >= 0)
      case "JUSTID"                      => justId = true
      case option @ ("FORCE" | "LASTID") => call.unsupported(option)
      case _                             => throw new Reject(Errors.syntax)
    }
    // a server replaces a delivery time before 0 or after now with now
    val at          = if (deliveredAt < 0 || deliveredAt > call.now) call.now else deliveredAt
    var pending     = g.pending
    val claimed     = ids.flatMap { id =>
      pending.get(id).filter(_.idleFor(minIdle, call.now)).flatMap { p =>
        s.entries.get(id) match {
          // an entry deleted from the stream leaves the pending list instead of being claimed
          case None         =>
            pending = pending - id
            None
          case Some(fields) =>
            pending = pending.updated(id, Pending(consumer, at, retryCount.getOrElse(if (justId) p.deliveries else p.deliveries + 1)))
            Some(id -> fields)
        }
      }
    }
    putGroup(call, s, touchConsumer(g.copy(pending = pending), consumer, call.now, active = claimed.nonEmpty))
    claimedFrame(claimed, justId)
  }

  private def claimedFrame(claimed: Vector[(StreamId, Vector[(Bytes, Bytes)])], justId: Boolean): Frame =
    if (justId) Frame.Array(claimed.map(entry => idFrame(entry._1))) else entriesFrame(claimed)

  private def xAutoClaim(call: Call): Frame = {
    val consumer   = call.text(2)
    val minIdle    = call.long(3)
    val start      = bound(call, 4, lower = true)
    var count      = 100L
    var justId     = false
    call.options(5) {
      case "COUNT"  => count = call.nextBounded(Errors.err("COUNT must be > 0"))(n => n >= 1 && n <= Long.MaxValue / 16)
      case "JUSTID" => justId = true
      case _        => throw new Reject(Errors.syntax)
    }
    val (s, g)     = group(call)
    var pending    = g.pending
    var claimed    = Vector.empty[(StreamId, Vector[(Bytes, Bytes)])]
    var deleted    = Vector.empty[StreamId]
    val candidates = g.pending.iteratorFrom(start)
    // like a server, this examines at most ten entries per claim, and an entry gone from the stream counts as a claim whatever its idle time
    var attempts   = count * 10
    while (attempts > 0 && claimed.length + deleted.length < count && candidates.hasNext) {
      val (id, p) = candidates.next()
      attempts -= 1
      s.entries.get(id) match {
        case None                                         =>
          pending -= id
          deleted :+= id
        case Some(fields) if p.idleFor(minIdle, call.now) =>
          pending = pending.updated(id, Pending(consumer, call.now, if (justId) p.deliveries else p.deliveries + 1))
          claimed :+= id -> fields
        case _                                            => ()
      }
    }
    val cursor     = candidates.nextOption().fold(StreamId.Zero)(_._1)
    putGroup(call, s, touchConsumer(g.copy(pending = pending), consumer, call.now, active = claimed.nonEmpty))
    Frame.Array(Vector(idFrame(cursor), claimedFrame(claimed, justId), Frame.Array(deleted.map(idFrame))))
  }

  // --- XINFO ------------------------------------------------------------------

  private def xInfoStream(call: Call): Frame = {
    if (call.args.length > 1) if (call.keyword(1) == "FULL") call.unsupported("FULL") else throw new Reject(Errors.syntax)
    val s                  = existing(call, call.key(0))
    fieldMap(
      "length"                  -> int(s.entries.size.toLong),
      "last-generated-id"       -> idFrame(s.lastId),
      "max-deleted-entry-id"    -> idFrame(s.maxDeletedId),
      "entries-added"           -> int(s.entriesAdded),
      "recorded-first-entry-id" -> idFrame(s.entries.headOption.fold(StreamId.Zero)(_._1)),
      "groups"                  -> int(s.groups.size.toLong),
      "first-entry"             -> s.entries.headOption.fold(Frame.Null)(entryFrame(_, _)),
      "last-entry"              -> s.entries.lastOption.fold(Frame.Null)(entryFrame(_, _))
    )
  }

  // entries-read is null, as a server reports it when it cannot tell, and lag counts the entries after the last delivered id
  private def xInfoGroups(call: Call): Frame = {
    val s = existing(call, call.key(0))
    Frame.Array(s.groups.toVector.map { (name, g) =>
      fieldMap(
        "name"              -> bulk(name),
        "consumers"         -> int(g.consumers.size.toLong),
        "pending"           -> int(g.pending.size.toLong),
        "last-delivered-id" -> idFrame(g.lastDelivered),
        "entries-read"      -> Frame.Null,
        "lag"               -> int(next(g.lastDelivered).fold(0)(s.entries.rangeFrom(_).size).toLong)
      )
    })
  }

  private def xInfoConsumers(call: Call): Frame = {
    val (_, g) = groupOf(call, Errors.noSuchKey)
    Frame.Array(g.consumers.toVector.map { (name, c) =>
      fieldMap(
        "name"     -> bulk(name),
        "pending"  -> int(g.pending.count((_, p) => p.consumer == name).toLong),
        "idle"     -> int(math.max(0L, call.now - c.seenAt)),
        "inactive" -> int(c.activeAt.fold(-1L)(call.now - _))
      )
    })
  }
}
