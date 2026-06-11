package sage.commands

import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS, TimeUnit}

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * Per-field outcome of HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT. `NoField` covers both an absent field and an absent key — the server
  * reports a missing key as a `-2` per requested field, never a top-level null.
  */
enum FieldExpiry {
  case NoField
  case ConditionNotMet
  case Updated
  case Deleted
}

enum FieldTtl {
  case NoField
  case NoExpiry
  case Expires(remaining: FiniteDuration)
}

enum FieldExpiryTime {
  case NoField
  case NoExpiry
  case At(timestamp: Instant)
}

enum FieldPersist {
  case NoField
  case NoExpiry
  case Persisted
}

/**
  * HSETEX's field-existence condition: `IfNoneExist` (FNX) writes only when none of the fields exist, `IfAllExist` (FXX) only when all do.
  * Distinct from key-level NX/XX and from the NX/XX/GT/LT of [[ExpireCondition]].
  */
enum HSetExCondition {
  case Always
  case IfNoneExist
  case IfAllExist
}

/**
  * A hash field is an identifier within the hash, never cluster-routed, so it takes a [[KeyCodec]] (not a [[ValueCodec]]): the same
  * representation-stability guarantee a key needs.
  */
private[sage] object Hashes {

  private val WithValues = Bytes.utf8("WITHVALUES")
  private val NoValues   = Bytes.utf8("NOVALUES")
  private val Fields     = Bytes.utf8("FIELDS")
  private val Fnx        = Bytes.utf8("FNX")
  private val Fxx        = Bytes.utf8("FXX")

  def hSet[K, F, V](key: K, first: (F, V), rest: (F, V)*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Long] =
    Command("HSET", Command.FirstKey, keyCodec.encode(key) +: fieldValueArgs(first +: rest.toVector), Decode.long)

  def hSetNx[K, F, V](key: K, field: F, value: V)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("HSETNX", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), valueCodec.encode(value)), Decode.flag)

  def hGet[K, F, V](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.read("HGET", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.optionalValue)

  def hmGet[K, F, V](key: K, first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command.read(
      "HMGET",
      Command.FirstKey,
      keyCodec.encode(key) +: (first +: rest.toVector).map(fieldCodec.encode),
      Decode.vector(Decode.optionalValue)
    )

  def hDel[K, F](key: K, first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HDEL", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(fieldCodec.encode), Decode.long)

  def hExists[K, F](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Boolean] =
    Command.read("HEXISTS", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.flag)

  def hLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("HLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def hStrLen[K, F](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command.read("HSTRLEN", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.long)

  def hKeys[K, F](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[F]] =
    Command.read("HKEYS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.vector(Decode.key[F]))

  def hVals[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.read("HVALS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.vector(Decode.value[V]))

  def hGetAll[K, F, V](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Map[F, V]] =
    Command.read("HGETALL", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.map[F, V])

  def hIncrBy[K, F](key: K, field: F, increment: Long)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HINCRBY", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), Bytes.utf8(increment.toString)), Decode.long)

  def hIncrByFloat[K, F](key: K, field: F, increment: Double)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Double] =
    Command(
      "HINCRBYFLOAT",
      Command.FirstKey,
      Vector(keyCodec.encode(key), fieldCodec.encode(field), Bytes.utf8(Doubles.format(increment))),
      Decode.double
    )

  def hRandField[K, F](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Option[F]] =
    Command.readUncacheable("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalKey[F])

  def hRandField[K, F](key: K, count: Long)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[F]] =
    Command.readUncacheable("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vector(Decode.key[F]))

  def hRandFieldWithValues[K, F, V](
    key: K,
    count: Long
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[(F, V)]] =
    Command.readUncacheable(
      "HRANDFIELD",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(count.toString), WithValues),
      Decode.nestedPairs[F, V]
    )

  def hScan[K, F, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[(F, V)]] =
    Command.read(
      "HSCAN",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ ScanArgs.options(pattern, count),
      Decode.scanPage(Decode.flatPairs[F, V])
    )

  def hScanNoValues[K, F](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[ScanPage[F]] =
    Command.read(
      "HSCAN",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ ScanArgs.options(pattern, count)) :+ NoValues,
      Decode.scanPage(Decode.vector(Decode.key[F]))
    )

  def hExpire[K, F](key: K, ttl: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[Vector[FieldExpiry]] = {
    val (name, amount) = TimeArgs.expireCommand("HEXPIRE", "HPEXPIRE", ttl)
    Command(
      name,
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ Keys.conditionArgs(condition) ++ fieldsArgs(first +: rest.toVector),
      Decode.vector(fieldExpiry)
    )
  }

  def hExpireAt[K, F](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[Vector[FieldExpiry]] = {
    val (name, amount) = TimeArgs.expireCommand("HEXPIREAT", "HPEXPIREAT", at)
    Command(
      name,
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ Keys.conditionArgs(condition) ++ fieldsArgs(first +: rest.toVector),
      Decode.vector(fieldExpiry)
    )
  }

  def hExpireTime[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldExpiryTime]] =
    Command.readUncacheable(
      "HEXPIRETIME",
      Command.FirstKey,
      keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector),
      Decode.vector(fieldExpiryTime(Instant.ofEpochSecond))
    )

  def hpExpireTime[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldExpiryTime]] =
    Command.readUncacheable(
      "HPEXPIRETIME",
      Command.FirstKey,
      keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector),
      Decode.vector(fieldExpiryTime(Instant.ofEpochMilli))
    )

  def hTtl[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldTtl]] =
    Command.readUncacheable("HTTL", Command.FirstKey, keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector), Decode.vector(fieldTtl(SECONDS)))

  def hpTtl[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldTtl]] =
    Command.readUncacheable(
      "HPTTL",
      Command.FirstKey,
      keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector),
      Decode.vector(fieldTtl(MILLISECONDS))
    )

  def hPersist[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldPersist]] =
    Command("HPERSIST", Command.FirstKey, keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector), Decode.vector(fieldPersist))

  def hGetDel[K, F, V](key: K)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command("HGETDEL", Command.FirstKey, keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector), Decode.vector(Decode.optionalValue))

  def hGetEx[K, F, V](key: K, expiry: GetExpiry = GetExpiry.Keep)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command(
      "HGETEX",
      Command.FirstKey,
      (keyCodec.encode(key) +: Strings.getExpiryArgs(expiry)) ++ fieldsArgs(first +: rest.toVector),
      Decode.vector(Decode.optionalValue)
    )

  def hSetEx[K, F, V](
    key: K,
    condition: HSetExCondition = HSetExCondition.Always,
    expiry: SetExpiry = SetExpiry.Clear
  )(first: (F, V), rest: (F, V)*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command(
      "HSETEX",
      Command.FirstKey,
      (keyCodec.encode(key) +: setExConditionArgs(condition)) ++ Strings.setExpiryArgs(expiry) ++ fieldValuePairsArgs(first +: rest.toVector),
      Decode.flag
    )

  private def fieldValueArgs[F, V](pairs: Vector[(F, V)])(using fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (field, value) => Vector(fieldCodec.encode(field), valueCodec.encode(value)) }

  private def fieldsArgs[F](fields: Vector[F])(using fieldCodec: KeyCodec[F]): Vector[Bytes] =
    Fields +: Bytes.utf8(fields.size.toString) +: fields.map(fieldCodec.encode)

  private def fieldValuePairsArgs[F, V](pairs: Vector[(F, V)])(using fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Vector[Bytes] =
    Fields +: Bytes.utf8(pairs.size.toString) +: fieldValueArgs(pairs)

  private def setExConditionArgs(condition: HSetExCondition): Vector[Bytes] =
    condition match {
      case HSetExCondition.Always      => Vector.empty
      case HSetExCondition.IfNoneExist => Vector(Fnx)
      case HSetExCondition.IfAllExist  => Vector(Fxx)
    }

  private val fieldExpiry: Frame => Either[DecodeError, FieldExpiry] = {
    case Frame.Integer(-2) => Right(FieldExpiry.NoField)
    case Frame.Integer(0)  => Right(FieldExpiry.ConditionNotMet)
    case Frame.Integer(1)  => Right(FieldExpiry.Updated)
    case Frame.Integer(2)  => Right(FieldExpiry.Deleted)
    case other             => Left(DecodeError("field expiry integer", Frame.describe(other)))
  }

  private def fieldTtl(unit: TimeUnit): Frame => Either[DecodeError, FieldTtl] = {
    case Frame.Integer(-2)                    => Right(FieldTtl.NoField)
    case Frame.Integer(-1)                    => Right(FieldTtl.NoExpiry)
    case Frame.Integer(amount) if amount >= 0 => Right(FieldTtl.Expires(FiniteDuration(amount, unit)))
    case other                                => Left(DecodeError("field ttl integer", Frame.describe(other)))
  }

  private def fieldExpiryTime(toInstant: Long => Instant): Frame => Either[DecodeError, FieldExpiryTime] = {
    case Frame.Integer(-2)                    => Right(FieldExpiryTime.NoField)
    case Frame.Integer(-1)                    => Right(FieldExpiryTime.NoExpiry)
    case Frame.Integer(amount) if amount >= 0 => Right(FieldExpiryTime.At(toInstant(amount)))
    case other                                => Left(DecodeError("field expiry time integer", Frame.describe(other)))
  }

  private val fieldPersist: Frame => Either[DecodeError, FieldPersist] = {
    case Frame.Integer(-2) => Right(FieldPersist.NoField)
    case Frame.Integer(-1) => Right(FieldPersist.NoExpiry)
    case Frame.Integer(1)  => Right(FieldPersist.Persisted)
    case other             => Left(DecodeError("field persist integer", Frame.describe(other)))
  }
}
