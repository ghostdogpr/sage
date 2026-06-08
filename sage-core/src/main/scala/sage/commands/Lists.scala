package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * Which end of a list to act on. Shared by `LMOVE` (both ends) and `LMPOP`: a list end is one domain primitive, not a per-command option
  * group, so the per-command-enum rule does not apply.
  */
enum ListSide {
  case Left, Right
}

object ListSide {

  private[commands] def wire(side: ListSide): Bytes =
    side match {
      case ListSide.Left  => LeftWord
      case ListSide.Right => RightWord
    }
  private val LeftWord                              = Bytes.utf8("LEFT")
  private val RightWord                             = Bytes.utf8("RIGHT")
}

enum InsertPosition {
  case Before, After
}

object Lists {

  def lPush[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    push("LPUSH", key, first +: rest.toVector)

  def rPush[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    push("RPUSH", key, first +: rest.toVector)

  def lPushX[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    push("LPUSHX", key, first +: rest.toVector)

  def rPushX[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    push("RPUSHX", key, first +: rest.toVector)

  def lPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("LPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def rPop[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("RPOP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def lPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command("LPOP", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vectorOrEmpty(Decode.value[V]))

  def rPopCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command("RPOP", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vectorOrEmpty(Decode.value[V]))

  def lLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("LLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def lRange[K, V](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command(
      "LRANGE",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(start.toString), Bytes.utf8(stop.toString)),
      Decode.vector(Decode.value[V])
    )

  def lIndex[K, V](key: K, index: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("LINDEX", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(index.toString)), Decode.optionalValue)

  def lSet[K, V](key: K, index: Long, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] =
    Command("LSET", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(index.toString), valueCodec.encode(value)), Decode.ok)

  // the list length after the insert; 0 if the key is absent, -1 if the pivot is not found
  def lInsert[K, V](key: K, position: InsertPosition, pivot: V, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "LINSERT",
      Command.FirstKey,
      Vector(keyCodec.encode(key), positionArg(position), valueCodec.encode(pivot), valueCodec.encode(value)),
      Decode.long
    )

  def lRem[K, V](key: K, count: Long, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("LREM", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString), valueCodec.encode(value)), Decode.long)

  def lTrim[K](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K]): Command[Unit] =
    Command("LTRIM", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(start.toString), Bytes.utf8(stop.toString)), Decode.ok)

  def lPos[K, V](key: K, element: V, rank: Option[Long] = None, maxLen: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[Long]] =
    Command(
      "LPOS",
      Command.FirstKey,
      Vector(keyCodec.encode(key), valueCodec.encode(element)) ++ rankArg(rank) ++ maxLenArg(maxLen),
      Decode.optionalLong
    )

  def lPosCount[K, V](key: K, element: V, count: Long, rank: Option[Long] = None, maxLen: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Long]] =
    Command(
      "LPOS",
      Command.FirstKey,
      Vector(keyCodec.encode(key), valueCodec.encode(element)) ++ rankArg(rank) ++ Vector(Count, Bytes.utf8(count.toString)) ++ maxLenArg(maxLen),
      Decode.vector(Decode.long)
    )

  def lMove[K, V](source: K, destination: K, from: ListSide, to: ListSide)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[V]] =
    Command(
      "LMOVE",
      Vector(0, 1),
      Vector(keyCodec.encode(source), keyCodec.encode(destination), ListSide.wire(from), ListSide.wire(to)),
      Decode.optionalValue
    )

  def lMpop[K, V](
    first: K,
    rest: K*
  )(side: ListSide, count: Option[Long] = None)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, Vector[V])]] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command(
      "LMPOP",
      keyIndices = Vector.tabulate(keys.size)(_ + 1),
      args = (Bytes.utf8(keys.size.toString) +: keys :+ ListSide.wire(side)) ++ count.toVector.flatMap(c => Vector(Count, Bytes.utf8(c.toString))),
      decode = {
        case Frame.Null                                 => Right(None)
        case Frame.Array(Vector(keyFrame, valuesFrame)) =>
          for {
            key    <- Decode.key[K](keyFrame)
            values <- Decode.vector(Decode.value[V])(valuesFrame)
          } yield Some((key, values))
        case other                                      => Left(DecodeError("array of key and values or null", Frame.describe(other)))
      }
    )
  }

  private def push[K, V](name: String, key: K, values: Vector[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(name, Command.FirstKey, keyCodec.encode(key) +: values.map(valueCodec.encode), Decode.long)

  private def positionArg(position: InsertPosition): Bytes =
    position match {
      case InsertPosition.Before => Before
      case InsertPosition.After  => After
    }

  private def rankArg(rank: Option[Long]): Vector[Bytes]     = rank.toVector.flatMap(r => Vector(Rank, Bytes.utf8(r.toString)))
  private def maxLenArg(maxLen: Option[Long]): Vector[Bytes] = maxLen.toVector.flatMap(m => Vector(MaxLen, Bytes.utf8(m.toString)))

  private val Before = Bytes.utf8("BEFORE")
  private val After  = Bytes.utf8("AFTER")
  private val Rank   = Bytes.utf8("RANK")
  private val Count  = Bytes.utf8("COUNT")
  private val MaxLen = Bytes.utf8("MAXLEN")
}
