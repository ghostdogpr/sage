package sage.commands

import sage.codec.{KeyCodec, ValueCodec}

/**
  * Set members are values (a [[ValueCodec]]), like list elements. The set-returning reads decode the RESP3 Set frame into a `Set[V]`;
  * `SRANDMEMBER` with a count decodes an Array instead, because a negative count deliberately yields duplicates a `Set` would collapse.
  */
private[sage] object Sets {

  def sAdd[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("SADD", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def sRem[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("SREM", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def sCard[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("SCARD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def sIsMember[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command.read("SISMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.flag)

  def sMisMember[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Boolean]] =
    Command.read("SMISMEMBER", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.vector(Decode.flag))

  def sMembers[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    Command.read("SMEMBERS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.set[V])

  def sMove[K, V](source: K, destination: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("SMOVE", Vector(0, 1), Vector(keyCodec.encode(source), keyCodec.encode(destination), valueCodec.encode(member)), Decode.flag)

  def sPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("SPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def sPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    Command("SPOP", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.set[V])

  def sRandMember[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.readUncacheable("SRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  // a negative count may repeat members, so the reply is an ordered Array, not a Set
  def sRandMemberCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.readUncacheable("SRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.vector(Decode.value[V]))

  def sDiff[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    KeyArgs.allKeys("SDIFF", first +: rest.toVector, Decode.set[V], readOnly = true)

  def sDiffStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    KeyArgs.allKeys("SDIFFSTORE", destination +: first +: rest.toVector, Decode.long)

  def sInter[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    KeyArgs.allKeys("SINTER", first +: rest.toVector, Decode.set[V], readOnly = true)

  def sInterStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    KeyArgs.allKeys("SINTERSTORE", destination +: first +: rest.toVector, Decode.long)

  def sInterCard[K](first: K, rest: K*)(limit: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    KeyArgs.interCard("SINTERCARD", first +: rest.toVector, limit)

  def sUnion[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Set[V]] =
    KeyArgs.allKeys("SUNION", first +: rest.toVector, Decode.set[V], readOnly = true)

  def sUnionStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    KeyArgs.allKeys("SUNIONSTORE", destination +: first +: rest.toVector, Decode.long)

  def sScan[K, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[V]] =
    KeyArgs.keyScan("SSCAN", key, cursor, pattern, count)(Decode.vector(Decode.value[V]))
}
