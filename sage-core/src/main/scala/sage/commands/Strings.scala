package sage.commands

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * SET's expiry options. `Clear` is the server default: a plain SET discards any existing TTL.
  */
enum SetExpiry {
  case Clear
  case In(duration: FiniteDuration)
  case At(timestamp: Instant)
  case KeepTtl
}

enum SetCondition {
  case Always
  case IfExists
  case IfNotExists
}

/**
  * GETEX's expiry options. `Keep` is the server default: a plain GETEX leaves the TTL untouched.
  */
enum GetExpiry {
  case Keep
  case In(duration: FiniteDuration)
  case At(timestamp: Instant)
  case Persist
}

object Strings {

  def append[K, V](key: K, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("APPEND", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(value)), Decode.long)

  def decr[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("DECR", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def decrBy[K](key: K, decrement: Long)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("DECRBY", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(decrement.toString)), Decode.long)

  def get[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("GET", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def getDel[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("GETDEL", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def getEx[K, V](key: K, expiry: GetExpiry = GetExpiry.Keep)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("GETEX", Command.FirstKey, keyCodec.encode(key) +: getExpiryArgs(expiry), Decode.optionalValue)

  def getRange[K, V](key: K, start: Long, end: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[V] =
    Command(
      "GETRANGE",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(start.toString), Bytes.utf8(end.toString)),
      Decode.value
    )

  def incr[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("INCR", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def incrBy[K](key: K, increment: Long)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("INCRBY", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(increment.toString)), Decode.long)

  def incrByFloat[K](key: K, increment: Double)(using keyCodec: KeyCodec[K]): Command[Double] =
    Command("INCRBYFLOAT", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(increment.toString)), Decode.double)

  def mGet[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[V]]] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command("MGET", keys.indices.toVector, keys, Decode.vector(Decode.optionalValue))
  }

  def mSet[K, V](first: (K, V), rest: (K, V)*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] =
    Command("MSET", msetKeyIndices(rest.size + 1), msetArgs(first +: rest.toVector), Decode.ok)

  def mSetNx[K, V](first: (K, V), rest: (K, V)*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("MSETNX", msetKeyIndices(rest.size + 1), msetArgs(first +: rest.toVector), Decode.flag)

  /**
    * False when `condition` made the server skip the write.
    */
  def set[K, V](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command(
      "SET",
      Command.FirstKey,
      Vector(keyCodec.encode(key), valueCodec.encode(value)) ++ conditionArgs(condition) ++ setExpiryArgs(expiry),
      decode = {
        case Frame.SimpleString("OK") => Right(true)
        case Frame.Null               => Right(false)
        case other                    => Left(DecodeError("simple string 'OK' or null", Frame.describe(other)))
      }
    )

  /**
    * SET with the GET option: returns the previous value. With `IfNotExists`, a `Some` therefore means the write was skipped.
    */
  def setGet[K, V](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command(
      "SET",
      Command.FirstKey,
      Vector(keyCodec.encode(key), valueCodec.encode(value)) ++ conditionArgs(condition) :+ Get :++ setExpiryArgs(expiry),
      Decode.optionalValue
    )

  def setRange[K, V](key: K, offset: Long, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "SETRANGE",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(offset.toString), valueCodec.encode(value)),
      Decode.long
    )

  def strLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("STRLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  private def conditionArgs(condition: SetCondition): Vector[Bytes] =
    condition match {
      case SetCondition.Always      => Vector.empty
      case SetCondition.IfExists    => Vector(Xx)
      case SetCondition.IfNotExists => Vector(Nx)
    }

  private def setExpiryArgs(expiry: SetExpiry): Vector[Bytes] =
    expiry match {
      case SetExpiry.Clear         => Vector.empty
      case SetExpiry.In(duration)  => TimeArgs.relative(duration)
      case SetExpiry.At(timestamp) => TimeArgs.absolute(timestamp)
      case SetExpiry.KeepTtl       => Vector(KeepTtl)
    }

  private def getExpiryArgs(expiry: GetExpiry): Vector[Bytes] =
    expiry match {
      case GetExpiry.Keep          => Vector.empty
      case GetExpiry.In(duration)  => TimeArgs.relative(duration)
      case GetExpiry.At(timestamp) => TimeArgs.absolute(timestamp)
      case GetExpiry.Persist       => Vector(Persist)
    }

  private def msetArgs[K, V](pairs: Vector[(K, V)])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (key, value) => Vector(keyCodec.encode(key), valueCodec.encode(value)) }

  private def msetKeyIndices(pairs: Int): Vector[Int] = Vector.tabulate(pairs)(_ * 2)
  private val Get                                     = Bytes.utf8("GET")
  private val Nx                                      = Bytes.utf8("NX")
  private val Xx                                      = Bytes.utf8("XX")
  private val KeepTtl                                 = Bytes.utf8("KEEPTTL")
  private val Persist                                 = Bytes.utf8("PERSIST")
}
