package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * Bitmap commands, which treat a string as an array of bits. Bit 0 is the most significant bit of the first byte, as on a real server.
  */
private[inmemory] object BitmapCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "BITCOUNT"    -> Spec(-2, bitCount),
    "BITFIELD"    -> Spec(-2, call => bitField(call, readOnly = false)),
    "BITFIELD_RO" -> Spec(-2, call => bitField(call, readOnly = true)),
    "BITOP"       -> Spec(-4, bitOp),
    "BITPOS"      -> Spec(-3, bitPos),
    "GETBIT"      -> Spec(3, getBit),
    "SETBIT"      -> Spec(4, setBit)
  )

  // the largest bit offset is 2^32 - 1, the size limit of a string
  private val MaxBits = 1L << 32

  private def offset(call: Call, index: Int): Long =
    Numbers
      .parseLong(call.args(index))
      .filter(o => o >= 0 && o < MaxBits)
      .getOrElse(throw new Reject(Errors.err("bit offset is not an integer or out of range")))

  private def bit(bytes: Array[Byte], position: Long): Boolean = {
    val index = (position >> 3).toInt
    index < bytes.length && (bytes(index) & (0x80 >> (position & 7).toInt)) != 0
  }

  private def write(call: Call, key: Key, bytes: Array[Byte]): Unit =
    call.store.put(key, Value.Str(Bytes.wrap(IArray.unsafeFromArray(bytes))), call.lookup(key).fold(Entry.NoExpiry)(_.expiresAt), call.now)

  private def current(call: Call, key: Key): Array[Byte] = call.string(key).fold(Array.emptyByteArray)(_.toArray)

  private def setBit(call: Call): Frame = {
    val key      = call.key(0)
    val position = offset(call, 1)
    val value    = call.text(2) match {
      case "0" => false
      case "1" => true
      case _   => throw new Reject(Errors.err("bit is not an integer or out of range"))
    }
    val base     = current(call, key)
    val bytes    = java.util.Arrays.copyOf(base, math.max(base.length, (position >> 3).toInt + 1))
    val previous = bit(bytes, position)
    val mask     = 0x80 >> (position & 7).toInt
    val index    = (position >> 3).toInt
    bytes(index) = (if (value) bytes(index) | mask else bytes(index) & ~mask).toByte
    write(call, key, bytes)
    flag(previous)
  }

  private def getBit(call: Call): Frame = flag(bit(current(call, call.key(0)), offset(call, 1)))

  // the inclusive range of bit positions selected by `start end [BYTE | BIT]` at `index`, or None when it is empty
  private def bitRange(call: Call, index: Int, length: Int, endGiven: Boolean): Option[(Long, Long)] = {
    val inBits = call.args.lift(index + 2).map(_.asUtf8String.toUpperCase(java.util.Locale.ROOT)) match {
      case None         => false
      case Some("BYTE") => false
      case Some("BIT")  => true
      case Some(_)      => throw new Reject(Errors.syntax)
    }
    val total  = if (inBits) length.toLong * 8 else length.toLong
    val start  = call.long(index)
    val end    = if (endGiven) call.long(index + 1) else total - 1
    val from   = math.max(0L, if (start < 0) total + start else start)
    val to     = math.min(total - 1, if (end < 0) total + end else end)
    Option.when(total > 0 && from <= to)(if (inBits) (from, to) else (from * 8, to * 8 + 7))
  }

  private def bitCount(call: Call): Frame = {
    val bytes = current(call, call.key(0))
    val range = call.args.length match {
      case 1     => Option.when(bytes.nonEmpty)((0L, bytes.length * 8L - 1))
      case 3 | 4 => bitRange(call, 1, bytes.length, endGiven = true)
      case _     => throw new Reject(Errors.syntax)
    }
    int(range.fold(0L)((from, to) => (from to to).count(bit(bytes, _)).toLong))
  }

  private def bitPos(call: Call): Frame = {
    val target = call.text(1) match {
      case "0" => false
      case "1" => true
      case _   => throw new Reject(Errors.err("The bit argument must be 1 or 0."))
    }
    val key    = call.key(0)
    if (call.lookup(key).isEmpty) int(if (target) -1L else 0L)
    else {
      val bytes    = current(call, key)
      val endGiven = call.args.length >= 4
      val range    = call.args.length match {
        case 2     => Option.when(bytes.nonEmpty)((0L, bytes.length * 8L - 1))
        case 3     => bitRange(call, 2, bytes.length, endGiven = false)
        case 4 | 5 => bitRange(call, 2, bytes.length, endGiven = true)
        case _     => throw new Reject(Errors.syntax)
      }
      range.fold(int(-1L)) { (from, to) =>
        (from to to).find(bit(bytes, _) == target) match {
          case Some(position)               => int(position)
          // looking for a clear bit without an explicit end finds the first bit after the string
          case None if !target && !endGiven => int(to + 1)
          case None                         => int(-1L)
        }
      }
    }
  }

  private def bitOp(call: Call): Frame = {
    val operation                           = call.keyword(0)
    if (!Set("AND", "OR", "XOR", "NOT").contains(operation)) throw new Reject(Errors.syntax)
    val destination                         = call.key(1)
    val sources                             = call.args.indices.drop(2).map(i => current(call, call.key(i)))
    if (operation == "NOT" && sources.length != 1) throw new Reject(Errors.err("BITOP NOT must be called with a single source key."))
    val length                              = if (sources.isEmpty) 0 else sources.map(_.length).max
    def at(bytes: Array[Byte], i: Int): Int = if (i < bytes.length) bytes(i) & 0xff else 0
    val result                              = Array.tabulate[Byte](length) { i =>
      val values = sources.map(at(_, i))
      (operation match {
        case "AND" => values.reduce(_ & _)
        case "OR"  => values.reduce(_ | _)
        case "XOR" => values.reduce(_ ^ _)
        case _     => ~values.head
      }).toByte
    }
    if (result.isEmpty) call.store.delete(destination, call.now): Unit
    else call.store.put(destination, Value.Str(Bytes.wrap(IArray.unsafeFromArray(result))), Entry.NoExpiry, call.now)
    int(result.length.toLong)
  }

  final private case class FieldType(signed: Boolean, bits: Int) {
    val min: BigInt = if (signed) -(BigInt(1) << (bits - 1)) else BigInt(0)
    val max: BigInt = if (signed) (BigInt(1) << (bits - 1)) - 1 else (BigInt(1) << bits) - 1
  }

  private def fieldType(call: Call, index: Int): FieldType = {
    val text   = call.text(index)
    val signed = text.headOption.contains('i')
    val bits   = Option.when(text.headOption.exists(c => c == 'i' || c == 'u'))(text.drop(1)).flatMap(_.toIntOption)
    bits match {
      case Some(n) if n >= 1 && (if (signed) n <= 64 else n <= 63) => FieldType(signed, n)
      case _                                                       =>
        throw new Reject(Errors.err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."))
    }
  }

  private def fieldOffset(call: Call, index: Int, width: Int): Long = {
    val text = call.text(index)
    val raw  =
      if (text.startsWith("#")) Numbers.parseLong(Bytes.utf8(text.drop(1))).map(_ * width)
      else Numbers.parseLong(call.args(index))
    raw.filter(o => o >= 0 && o + width <= MaxBits).getOrElse(throw new Reject(Errors.err("bit offset is not an integer or out of range")))
  }

  private def read(bytes: Array[Byte], at: Long, t: FieldType): BigInt = {
    val raw = (0 until t.bits).foldLeft(BigInt(0))((acc, i) => (acc << 1) | (if (bit(bytes, at + i)) 1 else 0))
    if (t.signed && raw.testBit(t.bits - 1)) raw - (BigInt(1) << t.bits) else raw
  }

  private def store(bytes: Array[Byte], at: Long, t: FieldType, value: BigInt): Array[Byte] = {
    val needed  = ((at + t.bits + 7) >> 3).toInt
    val result  = java.util.Arrays.copyOf(bytes, math.max(bytes.length, needed))
    val encoded = value & ((BigInt(1) << t.bits) - 1)
    (0 until t.bits).foreach { i =>
      val position = at + i
      val mask     = 0x80 >> (position & 7).toInt
      val index    = (position >> 3).toInt
      result(index) = (if (encoded.testBit(t.bits - 1 - i)) result(index) | mask else result(index) & ~mask).toByte
    }
    result
  }

  // applies the overflow mode to a value outside the type's range; None means FAIL refused it
  private def fit(value: BigInt, t: FieldType, mode: String): Option[BigInt] =
    if (value >= t.min && value <= t.max) Some(value)
    else
      mode match {
        case "SAT"  => Some(if (value > t.max) t.max else t.min)
        case "FAIL" => None
        case _      =>
          val span    = BigInt(1) << t.bits
          val wrapped = ((value - t.min) % span + span) % span + t.min
          Some(wrapped)
      }

  private def bitField(call: Call, readOnly: Boolean): Frame = {
    val key     = call.key(0)
    var bytes   = current(call, key)
    var mode    = "WRAP"
    var wrote   = false
    var replies = Vector.empty[Frame]
    var i       = 1
    while (i < call.args.length) {
      val op = call.keyword(i)
      if (readOnly && op != "GET") throw new Reject(Errors.err("BITFIELD_RO only supports the GET subcommand"))
      op match {
        case "GET" if i + 2 < call.args.length            =>
          val t = fieldType(call, i + 1)
          replies :+= int(read(bytes, fieldOffset(call, i + 2, t.bits), t).toLong)
          i += 3
        case "SET" | "INCRBY" if i + 3 < call.args.length =>
          val t        = fieldType(call, i + 1)
          val at       = fieldOffset(call, i + 2, t.bits)
          val operand  = BigInt(call.long(i + 3))
          val previous = read(bytes, at, t)
          fit(if (op == "SET") operand else previous + operand, t, mode) match {
            case None        => replies :+= Frame.Null
            case Some(value) =>
              bytes = store(bytes, at, t, value)
              wrote = true
              // SET replies with the old value and INCRBY with the new one
              replies :+= int((if (op == "SET") previous else value).toLong)
          }
          i += 4
        case "OVERFLOW" if i + 1 < call.args.length       =>
          mode = call.keyword(i + 1)
          if (!Set("WRAP", "SAT", "FAIL").contains(mode)) throw new Reject(Errors.err("Invalid OVERFLOW type specified"))
          i += 2
        case _                                            => throw new Reject(Errors.syntax)
      }
    }
    if (wrote) write(call, key, bytes)
    Frame.Array(replies)
  }
}

/**
  * HyperLogLog commands. A real server estimates cardinality with about 0.81% error; the in-memory client keeps the elements and counts
  * them exactly, which is one of the answers a server could give. The value is stored as a string that starts with `HYLL`, like a
  * server's, so `TYPE` reports `string` and `GET` and `SET` copy it. Other strings fail with `WRONGTYPE`, as they do on a server.
  */
private[inmemory] object HyperLogLogCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "PFADD"   -> Spec(-2, pfAdd),
    "PFCOUNT" -> Spec(-2, call => int(call.args.indices.flatMap(i => elements(call, call.key(i)).getOrElse(Set.empty)).toSet.size.toLong)),
    "PFMERGE" -> Spec(-2, pfMerge)
  )

  private val Magic = "HYLL".getBytes(java.nio.charset.StandardCharsets.US_ASCII)

  private val invalid = Frame.SimpleError("WRONGTYPE Key is not a valid HyperLogLog string value.")

  // the stored form: the magic bytes, the element count, then each element as its length and bytes
  private def encode(members: Set[Key]): Bytes = {
    val out  = new java.io.ByteArrayOutputStream()
    val data = new java.io.DataOutputStream(out)
    data.write(Magic)
    data.writeInt(members.size)
    members.toVector.sorted.foreach { member =>
      data.writeInt(member.bytes.length)
      data.write(member.bytes.unsafeArray)
    }
    Bytes.wrap(IArray.unsafeFromArray(out.toByteArray))
  }

  private def decode(bytes: Bytes): Option[Set[Key]] = {
    val raw = bytes.unsafeArray
    if (raw.length < Magic.length + 4 || !java.util.Arrays.equals(raw.take(Magic.length), Magic)) None
    else
      try {
        val data  = new java.io.DataInputStream(new java.io.ByteArrayInputStream(raw, Magic.length, raw.length - Magic.length))
        val count = data.readInt()
        Some(
          Vector
            .fill(count) {
              val element = new Array[Byte](data.readInt())
              data.readFully(element)
              new Key(Bytes.wrap(IArray.unsafeFromArray(element)))
            }
            .toSet
        )
      } catch { case _: java.io.IOException | _: NegativeArraySizeException => None }
  }

  private def elements(call: Call, key: Key): Option[Set[Key]] =
    call
      .lookup(key)
      .map(_.value match {
        case Value.Str(bytes) => decode(bytes).getOrElse(throw new Reject(invalid))
        case _                => throw new Reject(Errors.wrongType)
      })

  private def save(call: Call, key: Key, members: Set[Key]): Unit =
    call.store.put(key, Value.Str(encode(members)), call.lookup(key).fold(Entry.NoExpiry)(_.expiresAt), call.now)

  private def pfAdd(call: Call): Frame = {
    val key     = call.key(0)
    val current = elements(call, key)
    val updated = current.getOrElse(Set.empty) ++ call.args.indices.drop(1).map(call.key)
    val changed = current.forall(_ != updated)
    if (changed) save(call, key, updated)
    flag(changed)
  }

  private def pfMerge(call: Call): Frame = {
    val key = call.key(0)
    save(call, key, call.args.indices.flatMap(i => elements(call, call.key(i)).getOrElse(Set.empty)).toSet)
    ok
  }
}
