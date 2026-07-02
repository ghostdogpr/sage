package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * Set members are values (a [[ValueCodec]]), like list elements. The set-returning reads decode the RESP3 Set frame into a `Set[V]`;
  * `SRANDMEMBER` with a count decodes an Array instead, because a negative count deliberately yields duplicates a `Set` would collapse.
  */
private[sage] object Sets {

  private val Limit = Bytes.utf8("LIMIT")

  def sAdd[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("SADD", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode), Decode.long)

  def sRem[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("SREM", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode), Decode.long)

  def sCard[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("SCARD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def sIsMember[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command.read("SISMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.flag)

  def sMisMember[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Boolean]] =
    Command.read(
      "SMISMEMBER",
      Command.FirstKey,
      keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode),
      Decode.vector(Decode.flag)
    )

  def sMembers[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    Command.read("SMEMBERS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.set[V])

  def sMove[K, V](source: K, destination: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("SMOVE", Vector(0, 1), Vector(keyCodec.encode(source), keyCodec.encode(destination), valueCodec.encode(member)), Decode.flag)

  def sPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("SPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def sPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    Command("SPOP", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.set[V])

  def sRandMember[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.readUncacheable("SRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  // a negative count may repeat members, so the reply is an ordered Array, not a Set
  def sRandMemberCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.readUncacheable("SRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vector(Decode.value[V]))

  def sDiff[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    setOp("SDIFF", first +: rest.toVector, Decode.set[V])

  def sDiffStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    storeOp("SDIFFSTORE", destination, first +: rest.toVector)

  def sInter[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    setOp("SINTER", first +: rest.toVector, Decode.set[V])

  def sInterStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    storeOp("SINTERSTORE", destination, first +: rest.toVector)

  def sInterCard[K](first: K, rest: K*)(limit: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val (keyIndices, prefix) = KeyArgs.numKeyed(first +: rest.toVector)
    Command.read(
      "SINTERCARD",
      keyIndices,
      args = prefix ++ limit.toVector.flatMap(n => Vector(Limit, Bytes.utf8(n.toString))),
      Decode.long
    )
  }

  def sUnion[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    setOp("SUNION", first +: rest.toVector, Decode.set[V])

  def sUnionStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    storeOp("SUNIONSTORE", destination, first +: rest.toVector)

  def sScan[K, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[V]] =
    Command.readCursor(
      "SSCAN",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ ScanArgs.options(pattern, count),
      Decode.scanPage(Decode.vector(Decode.value[V]))
    )

  private def setOp[K, Out](name: String, keys: Vector[K], decode: Frame => Either[DecodeError, Out])(
    using keyCodec: KeyCodec[K]
  ): Command[Out] = {
    val args = keys.map(keyCodec.encode)
    Command.read(name, args.indices.toVector, args, decode)
  }

  private def storeOp[K](name: String, destination: K, keys: Vector[K])(using keyCodec: KeyCodec[K]): Command[Long] = {
    val args = (destination +: keys).map(keyCodec.encode)
    Command(name, args.indices.toVector, args, Decode.long)
  }
}
