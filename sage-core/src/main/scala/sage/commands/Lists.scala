package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.Count
import sage.protocol.Frame

/**
  * Selects the end of a list used by commands such as `LMOVE` and `LMPOP`. `Left` means the head, and `Right` means the tail.
  */
enum ListSide {
  case Left, Right
}

/**
  * Whether `LINSERT` places the new element `Before` or `After` the pivot.
  */
enum InsertPosition {
  case Before, After
}

private[sage] object Lists {

  private val sideArg     = Args.keywords(ListSide.values)
  private val positionArg = Args.keywords(InsertPosition.values)
  private val Rank        = Bytes.utf8("RANK")
  private val MaxLen      = Bytes.utf8("MAXLEN")

  def lPush[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("LPUSH", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def rPush[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("RPUSH", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def lPushX[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("LPUSHX", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def rPushX[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("RPUSHX", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def lPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("LPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def rPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("RPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def lPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command("LPOP", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.orEmpty(Decode.vector(Decode.value[V])))

  def rPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command("RPOP", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.orEmpty(Decode.vector(Decode.value[V])))

  def lLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("LLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def lRange[K, V](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.read(
      "LRANGE",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Args.long(start), Args.long(stop)),
      Decode.vector(Decode.value[V])
    )

  def lIndex[K, V](key: K, index: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.read("LINDEX", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(index)), Decode.optionalValue)

  def lSet[K, V](key: K, index: Long, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] =
    Command("LSET", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(index), valueCodec.encode(value)), Decode.ok)

  // the list length after the insert; 0 if the key is absent, -1 if the pivot is not found
  def lInsert[K, V](key: K, position: InsertPosition, pivot: V, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "LINSERT",
      Command.FirstKey,
      Vector(keyCodec.encode(key), positionArg(position), valueCodec.encode(pivot), valueCodec.encode(value)),
      Decode.long
    )

  def lRem[K, V](key: K, count: Long, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("LREM", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count), valueCodec.encode(value)), Decode.long)

  def lTrim[K](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K]): Command[Unit] =
    Command("LTRIM", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(start), Args.long(stop)), Decode.ok)

  def lPos[K, V](key: K, element: V, rank: Option[Long] = None, maxLen: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[Long]] =
    Command.read("LPOS", Command.FirstKey, posArgs(key, element, rank, Vector.empty, maxLen), Decode.optionalLong)

  def lPosCount[K, V](key: K, element: V, count: Long, rank: Option[Long] = None, maxLen: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Long]] =
    Command.read("LPOS", Command.FirstKey, posArgs(key, element, rank, Vector(Count, Args.long(count)), maxLen), Decode.vector(Decode.long))

  private def posArgs[K, V](key: K, element: V, rank: Option[Long], count: Vector[Bytes], maxLen: Option[Long])(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Vector[Bytes] =
    Vector(keyCodec.encode(key), valueCodec.encode(element)) ++ Args.optLong(Rank, rank) ++ count ++ Args.optLong(MaxLen, maxLen)

  def lMove[K, V](source: K, destination: K, from: ListSide, to: ListSide)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[V]] =
    Command(
      "LMOVE",
      Vector(0, 1),
      Vector(keyCodec.encode(source), keyCodec.encode(destination), sideArg(from), sideArg(to)),
      Decode.optionalValue
    )

  def lMpop[K, V](
    first: K,
    rest: K*
  )(side: ListSide, count: Option[Long] = None)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, Vector[V])]] =
    KeyArgs.multiPop("LMPOP", None, first +: rest.toVector, sideArg(side), count, Decode.vector(Decode.value[V]), MpopLabel)

  def blPop[K, V](first: K, rest: K*)(timeout: BlockTimeout)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V)]] =
    KeyArgs.blockingPop("BLPOP", first +: rest.toVector, timeout, poppedPair[K, V])

  def brPop[K, V](first: K, rest: K*)(timeout: BlockTimeout)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V)]] =
    KeyArgs.blockingPop("BRPOP", first +: rest.toVector, timeout, poppedPair[K, V])

  def blMove[K, V](source: K, destination: K, from: ListSide, to: ListSide, timeout: BlockTimeout)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[V]] =
    Command(
      "BLMOVE",
      Vector(0, 1),
      Vector(keyCodec.encode(source), keyCodec.encode(destination), sideArg(from), sideArg(to), BlockTimeout.wire(timeout)),
      Decode.optionalValue,
      Execution.Blocking
    )

  def blMpop[K, V](first: K, rest: K*)(side: ListSide, timeout: BlockTimeout, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, Vector[V])]] =
    KeyArgs.multiPop("BLMPOP", Some(timeout), first +: rest.toVector, sideArg(side), count, Decode.vector(Decode.value[V]), MpopLabel)

  private def poppedPair[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, (K, V)] =
    Decode.array2(Decode.key[K], Decode.value[V], "array of key and value or null")(_ -> _)

  private val MpopLabel = "array of key and values or null"
}
