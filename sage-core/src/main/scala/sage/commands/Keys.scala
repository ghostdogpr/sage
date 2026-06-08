package sage.commands

import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.protocol.Frame

enum ExpireCondition {
  case Always
  case IfNoExpiry
  case IfHasExpiry
  case IfGreater
  case IfLess
}

enum RedisType {
  case String, List, Set, ZSet, Hash, Stream
}

object RedisType {

  private[commands] def wireName(tpe: RedisType): java.lang.String =
    tpe match {
      case String => "string"
      case List   => "list"
      case Set    => "set"
      case ZSet   => "zset"
      case Hash   => "hash"
      case Stream => "stream"
    }

  private[commands] def fromWireName(name: java.lang.String): scala.Option[RedisType] =
    RedisType.values.find(wireName(_) == name)
}

enum Ttl {
  case NoKey
  case NoExpiry
  case Expires(remaining: FiniteDuration)
}

enum ExpiryTime {
  case NoKey
  case NoExpiry
  case At(timestamp: Instant)
}

/**
  * A server-issued SCAN position. Only `start` and cursors returned in a [[ScanPage]] are valid — there is nothing else to construct.
  */
opaque type ScanCursor = Bytes

object ScanCursor {

  val start: ScanCursor = Bytes.utf8("0")

  private[commands] def wrap(bytes: Bytes): ScanCursor = bytes

  private[commands] def bytes(cursor: ScanCursor): Bytes = cursor
}

/**
  * One cursor round-trip, shared by every SCAN-family command (`SCAN` keys, `HSCAN` field/value pairs, …). `next` is `None` when the
  * iteration is complete; empty `items` with a `Some` cursor is normal mid-iteration.
  */
final case class ScanPage[A](items: Vector[A], next: Option[ScanCursor])

object Keys {

  def copy[K](source: K, destination: K, replace: Boolean = false)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command(
      "COPY",
      keyIndices = Vector(0, 1),
      args = Vector(keyCodec.encode(source), keyCodec.encode(destination)) ++ (if (replace) Vector(Replace) else Vector.empty),
      Decode.flag
    )

  def del[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("DEL", first +: rest.toVector, Decode.long)

  def exists[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("EXISTS", first +: rest.toVector, Decode.long)

  def expire[K](key: K, in: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(using keyCodec: KeyCodec[K]): Command[Boolean] = {
    val (name, amount) = if (TimeArgs.wholeSeconds(in)) ("EXPIRE", in.toSeconds) else ("PEXPIRE", TimeArgs.millis(in))
    Command(name, Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ conditionArgs(condition), Decode.flag)
  }

  def expireAt[K](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(using keyCodec: KeyCodec[K]): Command[Boolean] = {
    val (name, amount) = if (TimeArgs.wholeSeconds(at)) ("EXPIREAT", at.getEpochSecond) else ("PEXPIREAT", TimeArgs.millis(at))
    Command(name, Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ conditionArgs(condition), Decode.flag)
  }

  def expireTime[K](key: K)(using keyCodec: KeyCodec[K]): Command[ExpiryTime] =
    Command("EXPIRETIME", Command.FirstKey, Vector(keyCodec.encode(key)), expiryTimeDecode(Instant.ofEpochSecond))

  def pExpireTime[K](key: K)(using keyCodec: KeyCodec[K]): Command[ExpiryTime] =
    Command("PEXPIRETIME", Command.FirstKey, Vector(keyCodec.encode(key)), expiryTimeDecode(Instant.ofEpochMilli))

  def keys[K](pattern: String)(using keyCodec: KeyCodec[K]): Command[Vector[K]] =
    Command("KEYS", Command.NoKeys, Vector(Bytes.utf8(pattern)), Decode.vector(Decode.key))

  def persist[K](key: K)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("PERSIST", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.flag)

  def pTtl[K](key: K)(using keyCodec: KeyCodec[K]): Command[Ttl] =
    Command("PTTL", Command.FirstKey, Vector(keyCodec.encode(key)), ttlDecode(MILLISECONDS))

  def randomKey[K](using keyCodec: KeyCodec[K]): Command[Option[K]] =
    Command("RANDOMKEY", Command.NoKeys, Vector.empty, Decode.optionalKey)

  def rename[K](source: K, destination: K)(using keyCodec: KeyCodec[K]): Command[Unit] =
    Command("RENAME", Vector(0, 1), Vector(keyCodec.encode(source), keyCodec.encode(destination)), Decode.ok)

  def renameNx[K](source: K, destination: K)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("RENAMENX", Vector(0, 1), Vector(keyCodec.encode(source), keyCodec.encode(destination)), Decode.flag)

  def scan[K](
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  )(using keyCodec: KeyCodec[K]): Command[ScanPage[K]] =
    Command(
      "SCAN",
      Command.NoKeys,
      ScanCursor.bytes(cursor) +:
        (pattern.toVector.flatMap(p => Vector(Match, Bytes.utf8(p))) ++
          count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString))) ++
          ofType.toVector.flatMap(t => Vector(Type, Bytes.utf8(RedisType.wireName(t))))),
      Decode.scanPage(Decode.vector(Decode.key[K]))
    )

  def touch[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("TOUCH", first +: rest.toVector, Decode.long)

  def ttl[K](key: K)(using keyCodec: KeyCodec[K]): Command[Ttl] =
    Command("TTL", Command.FirstKey, Vector(keyCodec.encode(key)), ttlDecode(SECONDS))

  def typeOf[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[RedisType]] =
    Command(
      "TYPE",
      Command.FirstKey,
      Vector(keyCodec.encode(key)),
      decode = {
        case Frame.SimpleString("none") => Right(None)
        case Frame.SimpleString(name)   =>
          RedisType.fromWireName(name).map(Some(_)).toRight(DecodeError("key type", s"simple string '$name'"))
        case other                      => Left(DecodeError("key type simple string", Frame.describe(other)))
      }
    )

  def unlink[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("UNLINK", first +: rest.toVector, Decode.long)

  private def allKeys[K, Out](name: String, keys: Vector[K], decode: Frame => Either[DecodeError, Out])(using keyCodec: KeyCodec[K]): Command[Out] = {
    val args = keys.map(keyCodec.encode)
    Command(name, args.indices.toVector, args, decode)
  }

  private def conditionArgs(condition: ExpireCondition): Vector[Bytes] =
    condition match {
      case ExpireCondition.Always      => Vector.empty
      case ExpireCondition.IfNoExpiry  => Vector(Nx)
      case ExpireCondition.IfHasExpiry => Vector(Xx)
      case ExpireCondition.IfGreater   => Vector(Gt)
      case ExpireCondition.IfLess      => Vector(Lt)
    }

  private def ttlDecode(unit: scala.concurrent.duration.TimeUnit): Frame => Either[DecodeError, Ttl] = {
    case Frame.Integer(-2)                    => Right(Ttl.NoKey)
    case Frame.Integer(-1)                    => Right(Ttl.NoExpiry)
    case Frame.Integer(amount) if amount >= 0 => Right(Ttl.Expires(FiniteDuration(amount, unit)))
    case other                                => Left(DecodeError("ttl integer", Frame.describe(other)))
  }

  private def expiryTimeDecode(toInstant: Long => Instant): Frame => Either[DecodeError, ExpiryTime] = {
    case Frame.Integer(-2)                    => Right(ExpiryTime.NoKey)
    case Frame.Integer(-1)                    => Right(ExpiryTime.NoExpiry)
    case Frame.Integer(amount) if amount >= 0 => Right(ExpiryTime.At(toInstant(amount)))
    case other                                => Left(DecodeError("expiry time integer", Frame.describe(other)))
  }
  private val Replace                                                                                = Bytes.utf8("REPLACE")
  private val Match                                                                                  = Bytes.utf8("MATCH")
  private val Count                                                                                  = Bytes.utf8("COUNT")
  private val Type                                                                                   = Bytes.utf8("TYPE")
  private val Nx                                                                                     = Bytes.utf8("NX")
  private val Xx                                                                                     = Bytes.utf8("XX")
  private val Gt                                                                                     = Bytes.utf8("GT")
  private val Lt                                                                                     = Bytes.utf8("LT")
}
