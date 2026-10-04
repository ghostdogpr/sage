package sage.commands

import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS, TimeUnit}

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.WithValues
import sage.protocol.Frame

/**
  * The outcome for one field in `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, or `HPEXPIREAT`. `NoField` represents a missing field or key. The server
  * reports a missing key as `-2` for each requested field instead of returning a top-level null.
  */
enum FieldExpiry {
  case NoField
  case ConditionNotMet
  case Updated
  case Deleted
}

/**
  * A per-field `HTTL`/`HPTTL` reply: the field is absent (`NoField`), has no expiry (`NoExpiry`), or `Expires` after `remaining`.
  */
enum FieldTtl {
  case NoField
  case NoExpiry
  case Expires(remaining: FiniteDuration)
}

/**
  * A per-field `HEXPIRETIME`/`HPEXPIRETIME` reply: the field is absent (`NoField`), has no expiry (`NoExpiry`), or expires `At` a timestamp.
  */
enum FieldExpiryTime {
  case NoField
  case NoExpiry
  case At(timestamp: Instant)
}

/**
  * A per-field `HPERSIST` reply: the field is absent (`NoField`), already had no expiry (`NoExpiry`), or was `Persisted` (expiry removed).
  */
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
  * Hash fields use [[KeyCodec]] because they are identifiers whose byte representation must remain stable. Cluster routing still uses only
  * the hash key.
  */
private[sage] object Hashes {

  private val NoValuesTail = Vector(Bytes.utf8("NOVALUES"))
  private val Fields       = Bytes.utf8("FIELDS")
  private val Fnx          = Bytes.utf8("FNX")
  private val Fxx          = Bytes.utf8("FXX")

  def hSet[K, F, V](key: K, first: (F, V), rest: (F, V)*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Long] =
    Command("HSET", Command.FirstKey, keyCodec.encode(key) +: Args.pairs(first +: rest.toVector), Decode.long)

  def hSetNx[K, F, V](key: K, field: F, value: V)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("HSETNX", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), valueCodec.encode(value)), Decode.flag)

  def hGet[K, F, V](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.read("HGET", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.optionalValue)

  def hmGet[K, F, V](key: K, first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command.read("HMGET", Command.FirstKey, Args.keyThen(key, first, rest)(fieldCodec.encode), Decode.vector(Decode.optionalValue))

  def hDel[K, F](key: K, first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HDEL", Command.FirstKey, Args.keyThen(key, first, rest)(fieldCodec.encode), Decode.long)

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
    Command("HINCRBY", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), Args.long(increment)), Decode.long)

  def hIncrByFloat[K, F](key: K, field: F, increment: Double)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Double] =
    Command(
      "HINCRBYFLOAT",
      Command.FirstKey,
      Vector(keyCodec.encode(key), fieldCodec.encode(field), Args.double(increment)),
      Decode.double
    )

  def hRandField[K, F](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Option[F]] =
    Command.readUncacheable("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalKey[F])

  def hRandField[K, F](key: K, count: Long)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[F]] =
    Command.readUncacheable("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.vector(Decode.key[F]))

  def hRandFieldWithValues[K, F, V](
    key: K,
    count: Long
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[(F, V)]] =
    Command.readUncacheable(
      "HRANDFIELD",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Args.long(count), WithValues),
      Decode.nestedPairs[F, V]
    )

  def hScan[K, F, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[(F, V)]] =
    KeyArgs.keyScan("HSCAN", key, cursor, pattern, count)(Decode.flatPairs[F, V])

  def hScanNoValues[K, F](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[ScanPage[F]] =
    KeyArgs.keyScan("HSCAN", key, cursor, pattern, count, NoValuesTail)(Decode.vector(Decode.key[F]))

  def hExpire[K, F](key: K, ttl: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[Vector[FieldExpiry]] =
    expireFields(TimeArgs.expireCommand("HEXPIRE", "HPEXPIRE", ttl), keyCodec.encode(key), condition, first +: rest.toVector)

  def hExpireAt[K, F](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[Vector[FieldExpiry]] =
    expireFields(TimeArgs.expireCommand("HEXPIREAT", "HPEXPIREAT", at), keyCodec.encode(key), condition, first +: rest.toVector)

  private def expireFields[F: KeyCodec](command: (String, Long), key: Bytes, condition: ExpireCondition, fields: Vector[F]) =
    Command(command._1, Command.FirstKey, Vector(key, Args.long(command._2)) ++ Keys.conditionArgs(condition) ++ fieldsArgs(fields), fieldExpiries)

  def hExpireTime[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldExpiryTime]] =
    Command.readUncacheable("HEXPIRETIME", Command.FirstKey, keyFields(key, first, rest), Decode.vector(fieldExpiryTime(Instant.ofEpochSecond)))

  def hpExpireTime[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldExpiryTime]] =
    Command.readUncacheable("HPEXPIRETIME", Command.FirstKey, keyFields(key, first, rest), Decode.vector(fieldExpiryTime(Instant.ofEpochMilli)))

  def hTtl[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldTtl]] =
    Command.readUncacheable("HTTL", Command.FirstKey, keyFields(key, first, rest), Decode.vector(fieldTtl(SECONDS)))

  def hpTtl[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldTtl]] =
    Command.readUncacheable("HPTTL", Command.FirstKey, keyFields(key, first, rest), Decode.vector(fieldTtl(MILLISECONDS)))

  def hPersist[K, F](key: K)(first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[FieldPersist]] =
    Command("HPERSIST", Command.FirstKey, keyFields(key, first, rest), Decode.vector(fieldPersist))

  def hGetDel[K, F, V](key: K)(first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command("HGETDEL", Command.FirstKey, keyFields(key, first, rest), Decode.vector(Decode.optionalValue))

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
    expiry: SetExpiry = SetExpiry.Clear,
    condition: HSetExCondition = HSetExCondition.Always
  )(first: (F, V), rest: (F, V)*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command(
      "HSETEX",
      Command.FirstKey,
      (keyCodec.encode(key) +: setExConditionArgs(condition)) ++ Strings.setExpiryArgs(expiry) ++ fieldValuePairsArgs(first +: rest.toVector),
      Decode.flag
    )

  private def keyFields[K, F](key: K, first: F, rest: Seq[F])(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Vector[Bytes] =
    keyCodec.encode(key) +: fieldsArgs(first +: rest.toVector)

  private def fieldsArgs[F](fields: Vector[F])(using fieldCodec: KeyCodec[F]): Vector[Bytes] =
    Fields +: Args.long(fields.size) +: fields.map(fieldCodec.encode)

  private def fieldValuePairsArgs[F, V](pairs: Vector[(F, V)])(using fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Vector[Bytes] =
    Fields +: Args.long(pairs.size) +: Args.pairs(pairs)

  private def setExConditionArgs(condition: HSetExCondition): Vector[Bytes] =
    condition match {
      case HSetExCondition.Always      => Vector.empty
      case HSetExCondition.IfNoneExist => Vector(Fnx)
      case HSetExCondition.IfAllExist  => Vector(Fxx)
    }

  private val fieldExpiries: Frame => Either[DecodeError, Vector[FieldExpiry]] = Decode.vector(Decode.shape("field expiry integer") {
    case Frame.Integer(-2) => Right(FieldExpiry.NoField)
    case Frame.Integer(0)  => Right(FieldExpiry.ConditionNotMet)
    case Frame.Integer(1)  => Right(FieldExpiry.Updated)
    case Frame.Integer(2)  => Right(FieldExpiry.Deleted)
  })

  private def fieldTtl(unit: TimeUnit): Frame => Either[DecodeError, FieldTtl] =
    Decode.expiryInteger(FieldTtl.NoField, FieldTtl.NoExpiry, "field ttl integer")(amount => FieldTtl.Expires(FiniteDuration(amount, unit)))

  private def fieldExpiryTime(toInstant: Long => Instant): Frame => Either[DecodeError, FieldExpiryTime] =
    Decode.expiryInteger(FieldExpiryTime.NoField, FieldExpiryTime.NoExpiry, "field expiry time integer")(amount =>
      FieldExpiryTime.At(toInstant(amount))
    )

  private val fieldPersist: Frame => Either[DecodeError, FieldPersist] = Decode.shape("field persist integer") {
    case Frame.Integer(-2) => Right(FieldPersist.NoField)
    case Frame.Integer(-1) => Right(FieldPersist.NoExpiry)
    case Frame.Integer(1)  => Right(FieldPersist.Persisted)
  }
}
