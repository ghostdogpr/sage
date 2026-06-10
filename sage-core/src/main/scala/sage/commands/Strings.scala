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

/**
  * A half-open-looking inclusive `[start, end]` offset pair within one of LCS's two inputs.
  */
final case class MatchRange(start: Long, end: Long)

/**
  * One aligned common run found by `LCS … IDX`: its span in each input, plus the run length when `WITHMATCHLEN` was requested.
  */
final case class LcsMatch(a: MatchRange, b: MatchRange, length: Option[Long])

final case class LcsMatches(matches: Vector[LcsMatch], length: Long)

/**
  * DELEX's compare-and-delete condition. `IfEq`/`IfNe` compare the stored value; `IfDigestEq`/`IfDigestNe` compare its hex digest as
  * returned by `DIGEST`.
  */
enum DelexCondition[+V] {
  case Always
  case IfEq(value: V)
  case IfNe(value: V)
  case IfDigestEq(digest: String)
  case IfDigestNe(digest: String)
}

/**
  * INCREX's reply: the new value, and the amount actually applied — 0 when an out-of-bounds increment was rejected, or the capped delta under SATURATE.
  */
final case class IncrExResult[N](value: N, applied: N)

/**
  * INCREX's expiry options. `onlyIfNoTtl` (ENX) sets the expiry only when the key currently has none; it is a field on `In`/`At` because
  * the server rejects ENX without one of EX/PX/EXAT/PXAT.
  */
enum IncrExpiry {
  case Keep
  case In(duration: FiniteDuration, onlyIfNoTtl: Boolean = false)
  case At(timestamp: Instant, onlyIfNoTtl: Boolean = false)
  case Persist
}

private[sage] object Strings {

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

  def lcs[K, V](key1: K, key2: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[V] =
    Command("LCS", Vector(0, 1), Vector(keyCodec.encode(key1), keyCodec.encode(key2)), Decode.value)

  def lcsLen[K](key1: K, key2: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("LCS", Vector(0, 1), Vector(keyCodec.encode(key1), keyCodec.encode(key2), Len), Decode.long)

  def lcsIdx[K](key1: K, key2: K, minMatchLen: Option[Long] = None, withMatchLen: Boolean = false)(
    using keyCodec: KeyCodec[K]
  ): Command[LcsMatches] =
    Command(
      "LCS",
      Vector(0, 1),
      Vector(keyCodec.encode(key1), keyCodec.encode(key2), Idx) ++
        minMatchLen.toVector.flatMap(n => Vector(MinMatchLen, Bytes.utf8(n.toString))) ++
        (if (withMatchLen) Vector(WithMatchLen) else Vector.empty),
      lcsMatches
    )

  def digest[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[String]] =
    Command("DIGEST", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalUtf8String)

  def delex[K, V](key: K, condition: DelexCondition[V] = DelexCondition.Always)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Boolean] =
    Command("DELEX", Command.FirstKey, keyCodec.encode(key) +: delexConditionArgs(condition), Decode.flag)

  def msetEx[K, V](condition: SetCondition = SetCondition.Always, expiry: SetExpiry = SetExpiry.Clear)(
    first: (K, V),
    rest: (K, V)*
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] = {
    val pairs = first +: rest.toVector
    val data  = pairs.flatMap { case (key, value) => Vector(keyCodec.encode(key), valueCodec.encode(value)) }
    Command(
      "MSETEX",
      Vector.tabulate(pairs.size)(i => 1 + i * 2),
      (Bytes.utf8(pairs.size.toString) +: data) ++ conditionArgs(condition) ++ setExpiryArgs(expiry),
      Decode.flag
    )
  }

  def increxBy[K](
    key: K,
    increment: Long = 1,
    saturate: Boolean = false,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  )(using keyCodec: KeyCodec[K]): Command[IncrExResult[Long]] =
    Command(
      "INCREX",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ByInt, Bytes.utf8(increment.toString)) ++
        incrExArgs(saturate, lowerBound.map(_.toString), upperBound.map(_.toString), expiry),
      incrExResultLong
    )

  def increxByFloat[K](
    key: K,
    increment: Double,
    saturate: Boolean = false,
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  )(using keyCodec: KeyCodec[K]): Command[IncrExResult[Double]] =
    Command(
      "INCREX",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ByFloat, Bytes.utf8(increment.toString)) ++
        incrExArgs(saturate, lowerBound.map(_.toString), upperBound.map(_.toString), expiry),
      incrExResultDouble
    )

  private def incrExArgs(saturate: Boolean, lowerBound: Option[String], upperBound: Option[String], expiry: IncrExpiry): Vector[Bytes] =
    (if (saturate) Vector(Saturate) else Vector.empty) ++
      lowerBound.toVector.flatMap(x => Vector(LBound, Bytes.utf8(x))) ++
      upperBound.toVector.flatMap(x => Vector(UBound, Bytes.utf8(x))) ++
      incrExpiryArgs(expiry)

  private def incrExpiryArgs(expiry: IncrExpiry): Vector[Bytes] =
    expiry match {
      case IncrExpiry.Keep                     => Vector.empty
      case IncrExpiry.In(duration, onlyNoTtl)  => TimeArgs.relative(duration) ++ (if (onlyNoTtl) Vector(Enx) else Vector.empty)
      case IncrExpiry.At(timestamp, onlyNoTtl) => TimeArgs.absolute(timestamp) ++ (if (onlyNoTtl) Vector(Enx) else Vector.empty)
      case IncrExpiry.Persist                  => Vector(Persist)
    }

  private def delexConditionArgs[V](condition: DelexCondition[V])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    condition match {
      case DelexCondition.Always           => Vector.empty
      case DelexCondition.IfEq(value)      => Vector(IfEq, valueCodec.encode(value))
      case DelexCondition.IfNe(value)      => Vector(IfNe, valueCodec.encode(value))
      case DelexCondition.IfDigestEq(hash) => Vector(IfDeq, Bytes.utf8(hash))
      case DelexCondition.IfDigestNe(hash) => Vector(IfDne, Bytes.utf8(hash))
    }

  private val matchRange: Frame => Either[DecodeError, MatchRange] = {
    case Frame.Array(Vector(Frame.Integer(start), Frame.Integer(end))) => Right(MatchRange(start, end))
    case other                                                         => Left(DecodeError("match range [start, end]", Frame.describe(other)))
  }

  private val lcsMatch: Frame => Either[DecodeError, LcsMatch] = {
    case Frame.Array(Vector(a, b))                     =>
      for {
        ar <- matchRange(a)
        br <- matchRange(b)
      } yield LcsMatch(ar, br, None)
    case Frame.Array(Vector(a, b, Frame.Integer(len))) =>
      for {
        ar <- matchRange(a)
        br <- matchRange(b)
      } yield LcsMatch(ar, br, Some(len))
    case other                                         => Left(DecodeError("lcs match", Frame.describe(other)))
  }

  private val lcsMatches: Frame => Either[DecodeError, LcsMatches] = {
    case Frame.Map(entries) =>
      val lookup = entries.collect { case (Frame.BulkString(name), value) => name.asUtf8String -> value }.toMap
      for {
        matchesFrame <- lookup.get("matches").toRight(DecodeError("lcs idx 'matches' field", "map without 'matches'"))
        lenFrame     <- lookup.get("len").toRight(DecodeError("lcs idx 'len' field", "map without 'len'"))
        matches      <- Decode.vector(lcsMatch)(matchesFrame)
        len          <- Decode.long(lenFrame)
      } yield LcsMatches(matches, len)
    case other              => Left(DecodeError("lcs idx map", Frame.describe(other)))
  }

  private val incrExResultLong: Frame => Either[DecodeError, IncrExResult[Long]] = {
    case Frame.Array(Vector(Frame.Integer(value), Frame.Integer(applied))) => Right(IncrExResult(value, applied))
    case other                                                             => Left(DecodeError("INCREX [value, applied] integers", Frame.describe(other)))
  }

  private val incrExResultDouble: Frame => Either[DecodeError, IncrExResult[Double]] = {
    case Frame.Array(Vector(valueFrame, appliedFrame)) =>
      for {
        value   <- Decode.score(valueFrame)
        applied <- Decode.score(appliedFrame)
      } yield IncrExResult(value, applied)
    case other                                         => Left(DecodeError("INCREX [value, applied] doubles", Frame.describe(other)))
  }

  private def conditionArgs(condition: SetCondition): Vector[Bytes] =
    condition match {
      case SetCondition.Always      => Vector.empty
      case SetCondition.IfExists    => Vector(Xx)
      case SetCondition.IfNotExists => Vector(Nx)
    }

  private[commands] def setExpiryArgs(expiry: SetExpiry): Vector[Bytes] =
    expiry match {
      case SetExpiry.Clear         => Vector.empty
      case SetExpiry.In(duration)  => TimeArgs.relative(duration)
      case SetExpiry.At(timestamp) => TimeArgs.absolute(timestamp)
      case SetExpiry.KeepTtl       => Vector(KeepTtl)
    }

  private[commands] def getExpiryArgs(expiry: GetExpiry): Vector[Bytes] =
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
  private val Len                                     = Bytes.utf8("LEN")
  private val Idx                                     = Bytes.utf8("IDX")
  private val MinMatchLen                             = Bytes.utf8("MINMATCHLEN")
  private val WithMatchLen                            = Bytes.utf8("WITHMATCHLEN")
  private val ByInt                                   = Bytes.utf8("BYINT")
  private val ByFloat                                 = Bytes.utf8("BYFLOAT")
  private val Saturate                                = Bytes.utf8("SATURATE")
  private val LBound                                  = Bytes.utf8("LBOUND")
  private val UBound                                  = Bytes.utf8("UBOUND")
  private val Enx                                     = Bytes.utf8("ENX")
  private val IfEq                                    = Bytes.utf8("IFEQ")
  private val IfNe                                    = Bytes.utf8("IFNE")
  private val IfDeq                                   = Bytes.utf8("IFDEQ")
  private val IfDne                                   = Bytes.utf8("IFDNE")
}
