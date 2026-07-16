package sage.commands

import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * A guard on an `EXPIRE`-family write: `Always`, only `IfNoExpiry` (`NX`), only `IfHasExpiry` (`XX`), only `IfGreater` than the current
  * expiry (`GT`), or only `IfLess` (`LT`). The new expiry is applied only when the condition holds.
  */
enum ExpireCondition {
  case Always
  case IfNoExpiry
  case IfHasExpiry
  case IfGreater
  case IfLess
}

/**
  * The data type a key holds, as reported by `TYPE`. `Other` carries the wire name of any type this library does not classify as one of the
  * six classic types — a preview type (Arrays, vector sets) or a module type — so `TYPE` never fails to decode.
  */
enum RedisType {
  case String, List, Set, ZSet, Hash, Stream
  case Other(name: java.lang.String)
}

object RedisType {

  private[commands] def wireName(tpe: RedisType): java.lang.String =
    tpe match {
      case String      => "string"
      case List        => "list"
      case Set         => "set"
      case ZSet        => "zset"
      case Hash        => "hash"
      case Stream      => "stream"
      case Other(name) => name
    }

  private[commands] def fromWireName(name: java.lang.String): RedisType =
    name match {
      case "string" => String
      case "list"   => List
      case "set"    => Set
      case "zset"   => ZSet
      case "hash"   => Hash
      case "stream" => Stream
      case other    => Other(other)
    }
}

/**
  * A `TTL`/`PTTL` reply: the key is absent (`NoKey`), exists with no expiry set (`NoExpiry`), or `Expires` after `remaining`.
  */
enum Ttl {
  case NoKey
  case NoExpiry
  case Expires(remaining: FiniteDuration)
}

/**
  * An `EXPIRETIME`/`PEXPIRETIME` reply: the key is absent (`NoKey`), has no expiry (`NoExpiry`), or expires `At` an absolute timestamp.
  */
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

/**
  * Ascending or descending order for `SORT`.
  */
enum SortOrder {
  case Asc
  case Desc
}

/**
  * RESTORE's TTL. `NoExpiry` is the wire's `0`; `At` sets `ABSTTL` so the duration is read as an absolute timestamp.
  */
enum RestoreExpiry {
  case NoExpiry
  case In(duration: FiniteDuration)
  case At(timestamp: Instant)
}

/**
  * MIGRATE's authentication: `Password` is the legacy `AUTH password` (default user), `UserPassword` the ACL `AUTH2 username password`.
  */
enum MigrateAuth {
  case None
  case Password(password: String)
  case UserPassword(username: String, password: String)
}

/**
  * A `MIGRATE` outcome: `Ok` when the key was transferred, `NoKey` when the source held nothing to migrate.
  */
enum MigrateResult {
  case Ok
  case NoKey
}

private[sage] object Keys {

  private val Replace   = Bytes.utf8("REPLACE")
  private val Type      = Bytes.utf8("TYPE")
  private val Nx        = Bytes.utf8("NX")
  private val Xx        = Bytes.utf8("XX")
  private val Gt        = Bytes.utf8("GT")
  private val Lt        = Bytes.utf8("LT")
  private val By        = Bytes.utf8("BY")
  private val Get       = Bytes.utf8("GET")
  private val LimitWord = Bytes.utf8("LIMIT")
  private val Desc      = Bytes.utf8("DESC")
  private val Alpha     = Bytes.utf8("ALPHA")
  private val Store     = Bytes.utf8("STORE")
  private val AbsTtl    = Bytes.utf8("ABSTTL")
  private val IdleTime  = Bytes.utf8("IDLETIME")
  private val Freq      = Bytes.utf8("FREQ")
  private val Copy      = Bytes.utf8("COPY")
  private val Auth      = Bytes.utf8("AUTH")
  private val Auth2     = Bytes.utf8("AUTH2")
  private val KeysWord  = Bytes.utf8("KEYS")
  private val Encoding  = Bytes.utf8("ENCODING")
  private val RefCount  = Bytes.utf8("REFCOUNT")

  def copy[K](source: K, destination: K, replace: Boolean = false)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command(
      "COPY",
      keyIndices = Vector(0, 1),
      args = Vector(keyCodec.encode(source), keyCodec.encode(destination)) ++ (if (replace) Vector(Replace) else Vector.empty),
      Decode.flag
    )

  def del[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("DEL", first +: rest.toVector, Decode.long)

  // Valkey's atomic compare-and-delete: removes the key only if its current string value equals `value`
  def delIfEq[K, V](key: K, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("DELIFEQ", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(value)), Decode.flag)

  def exists[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("EXISTS", first +: rest.toVector, Decode.long, readOnly = true)

  def expire[K](key: K, in: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(using keyCodec: KeyCodec[K]): Command[Boolean] = {
    val (name, amount) = TimeArgs.expireCommand("EXPIRE", "PEXPIRE", in)
    Command(name, Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ conditionArgs(condition), Decode.flag)
  }

  def expireAt[K](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(using keyCodec: KeyCodec[K]): Command[Boolean] = {
    val (name, amount) = TimeArgs.expireCommand("EXPIREAT", "PEXPIREAT", at)
    Command(name, Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(amount.toString)) ++ conditionArgs(condition), Decode.flag)
  }

  def expireTime[K](key: K)(using keyCodec: KeyCodec[K]): Command[ExpiryTime] =
    Command.readUncacheable("EXPIRETIME", Command.FirstKey, Vector(keyCodec.encode(key)), expiryTimeDecode(Instant.ofEpochSecond))

  def pExpireTime[K](key: K)(using keyCodec: KeyCodec[K]): Command[ExpiryTime] =
    Command.readUncacheable("PEXPIRETIME", Command.FirstKey, Vector(keyCodec.encode(key)), expiryTimeDecode(Instant.ofEpochMilli))

  // KEYS is node-local; `allMasters` + `Concat` makes a cluster sweep every master and merge the slices. Prefer `scanAll` in production.
  def keys[K](pattern: String)(using keyCodec: KeyCodec[K]): Command[Vector[K]] =
    Command(
      "KEYS",
      Command.NoKeys,
      Vector(Bytes.utf8(pattern)),
      Decode.vector(Decode.key),
      isReadOnly = true,
      allMasters = true,
      broadcast = BroadcastReduce.Concat
    )

  def persist[K](key: K)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("PERSIST", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.flag)

  def pTtl[K](key: K)(using keyCodec: KeyCodec[K]): Command[Ttl] =
    Command.readUncacheable("PTTL", Command.FirstKey, Vector(keyCodec.encode(key)), ttlDecode(MILLISECONDS))

  def randomKey[K](using keyCodec: KeyCodec[K]): Command[Option[K]] =
    Command.readUncacheable("RANDOMKEY", Command.NoKeys, Vector.empty, Decode.optionalKey)

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
    Command.readCursor(
      "SCAN",
      Command.NoKeys,
      ScanCursor.bytes(cursor) +:
        (ScanArgs.options(pattern, count) ++
          ofType.toVector.flatMap(t => Vector(Type, Bytes.utf8(RedisType.wireName(t))))),
      Decode.scanPage(Decode.vector(Decode.key[K]))
    )

  def touch[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("TOUCH", first +: rest.toVector, Decode.long)

  def ttl[K](key: K)(using keyCodec: KeyCodec[K]): Command[Ttl] =
    Command.readUncacheable("TTL", Command.FirstKey, Vector(keyCodec.encode(key)), ttlDecode(SECONDS))

  def typeOf[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[RedisType]] =
    Command.read(
      "TYPE",
      Command.FirstKey,
      Vector(keyCodec.encode(key)),
      decode = {
        case Frame.SimpleString("none") => Right(None)
        case Frame.SimpleString(name)   => Right(Some(RedisType.fromWireName(name)))
        case other                      => Left(DecodeError("key type simple string", Frame.describe(other)))
      }
    )

  def unlink[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] =
    allKeys("UNLINK", first +: rest.toVector, Decode.long)

  def sort[K, V](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[V]]] =
    Command("SORT", Command.FirstKey, keyCodec.encode(key) +: sortOptionArgs(by, limit, get, order, alpha), Decode.vector(Decode.optionalValue))

  def sortStore[K](
    destination: K,
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  )(using keyCodec: KeyCodec[K]): Command[Long] = {
    val args = ((keyCodec.encode(key) +: sortOptionArgs(by, limit, get, order, alpha)) :+ Store) :+ keyCodec.encode(destination)
    Command("SORT", Vector(0, args.size - 1), args, Decode.long)
  }

  def sortRo[K, V](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[V]]] = {
    val args   = keyCodec.encode(key) +: sortOptionArgs(by, limit, get, order, alpha)
    val decode = Decode.vector(Decode.optionalValue)
    // BY/GET patterns dereference keys beyond the source key, which the cache's reverse index (built from keyIndices) does
    // not track, so an invalidation for one would not evict — only the bare form reads the source key alone and is cacheable
    if (by.isEmpty && get.isEmpty) Command.read("SORT_RO", Command.FirstKey, args, decode)
    else Command.readUncacheable("SORT_RO", Command.FirstKey, args, decode)
  }

  def move[K](key: K, db: Int)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("MOVE", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(db.toString)), Decode.flag)

  def dump[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[Bytes]] =
    Command.read("DUMP", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalBytes)

  def restore[K](
    key: K,
    payload: Bytes,
    expiry: RestoreExpiry = RestoreExpiry.NoExpiry,
    replace: Boolean = false,
    idleTime: Option[FiniteDuration] = None,
    freq: Option[Long] = None
  )(using keyCodec: KeyCodec[K]): Command[Unit] = {
    val (ttl, absTtl) = expiry match {
      case RestoreExpiry.NoExpiry     => (0L, false)
      case RestoreExpiry.In(duration) => (TimeArgs.millis(duration), false)
      case RestoreExpiry.At(at)       => (TimeArgs.millis(at), true)
    }
    Command(
      "RESTORE",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(ttl.toString), payload) ++
        (if (replace) Vector(Replace) else Vector.empty) ++
        (if (absTtl) Vector(AbsTtl) else Vector.empty) ++
        idleTime.toVector.flatMap(d => Vector(IdleTime, Bytes.utf8(d.toSeconds.toString))) ++
        freq.toVector.flatMap(f => Vector(Freq, Bytes.utf8(f.toString))),
      Decode.ok
    )
  }

  def migrate[K](
    host: String,
    port: Int,
    destinationDb: Int,
    timeout: FiniteDuration,
    copy: Boolean = false,
    replace: Boolean = false,
    auth: MigrateAuth = MigrateAuth.None
  )(first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[MigrateResult] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    val args =
      Vector(Bytes.utf8(host), Bytes.utf8(port.toString), Bytes.empty, Bytes.utf8(destinationDb.toString), Bytes.utf8(timeout.toMillis.toString)) ++
        (if (copy) Vector(Copy) else Vector.empty) ++
        (if (replace) Vector(Replace) else Vector.empty) ++
        authArgs(auth) ++
        (KeysWord +: keys)
    Command("MIGRATE", Vector.range(args.size - keys.size, args.size), args, migrateResult)
  }

  def objectEncoding[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[String]] =
    Command.read("OBJECT", Vector(1), Vector(Encoding, keyCodec.encode(key)), Decode.optionalUtf8String)

  def objectRefCount[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[Long]] =
    Command.readUncacheable("OBJECT", Vector(1), Vector(RefCount, keyCodec.encode(key)), Decode.optionalLong)

  def objectFreq[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[Long]] =
    Command.readUncacheable("OBJECT", Vector(1), Vector(Freq, keyCodec.encode(key)), Decode.optionalLong)

  def objectIdleTime[K](key: K)(using keyCodec: KeyCodec[K]): Command[Option[FiniteDuration]] =
    Command.readUncacheable("OBJECT", Vector(1), Vector(IdleTime, keyCodec.encode(key)), Decode.optionalLong).map(_.map(FiniteDuration(_, SECONDS)))

  private def allKeys[K, Out](name: String, keys: Vector[K], decode: Frame => Either[DecodeError, Out], readOnly: Boolean = false)(
    using keyCodec: KeyCodec[K]
  ): Command[Out] = {
    val args = keys.map(keyCodec.encode)
    if (readOnly) Command.read(name, args.indices.toVector, args, decode)
    else Command(name, args.indices.toVector, args, decode)
  }

  private def sortOptionArgs(by: Option[String], limit: Option[Limit], get: Vector[String], order: SortOrder, alpha: Boolean): Vector[Bytes] =
    by.toVector.flatMap(p => Vector(By, Bytes.utf8(p))) ++
      limit.toVector.flatMap(l => Vector(LimitWord, Bytes.utf8(l.offset.toString), Bytes.utf8(l.count.toString))) ++
      get.flatMap(p => Vector(Get, Bytes.utf8(p))) ++
      (order match {
        case SortOrder.Asc  => Vector.empty
        case SortOrder.Desc => Vector(Desc)
      }) ++
      (if (alpha) Vector(Alpha) else Vector.empty)

  private def authArgs(auth: MigrateAuth): Vector[Bytes] =
    auth match {
      case MigrateAuth.None                         => Vector.empty
      case MigrateAuth.Password(password)           => Vector(Auth, Bytes.utf8(password))
      case MigrateAuth.UserPassword(user, password) => Vector(Auth2, Bytes.utf8(user), Bytes.utf8(password))
    }

  private val migrateResult: Frame => Either[DecodeError, MigrateResult] = {
    case Frame.SimpleString("OK")    => Right(MigrateResult.Ok)
    case Frame.SimpleString("NOKEY") => Right(MigrateResult.NoKey)
    case other                       => Left(DecodeError("simple string 'OK' or 'NOKEY'", Frame.describe(other)))
  }

  private[commands] def conditionArgs(condition: ExpireCondition): Vector[Bytes] =
    condition match {
      case ExpireCondition.Always      => Vector.empty
      case ExpireCondition.IfNoExpiry  => Vector(Nx)
      case ExpireCondition.IfHasExpiry => Vector(Xx)
      case ExpireCondition.IfGreater   => Vector(Gt)
      case ExpireCondition.IfLess      => Vector(Lt)
    }

  private def ttlDecode(unit: scala.concurrent.duration.TimeUnit): Frame => Either[DecodeError, Ttl] =
    Decode.expiryInteger(Ttl.NoKey, Ttl.NoExpiry, "ttl integer")(amount => Ttl.Expires(FiniteDuration(amount, unit)))

  private def expiryTimeDecode(toInstant: Long => Instant): Frame => Either[DecodeError, ExpiryTime] =
    Decode.expiryInteger(ExpiryTime.NoKey, ExpiryTime.NoExpiry, "expiry time integer")(amount => ExpiryTime.At(toInstant(amount)))
}
