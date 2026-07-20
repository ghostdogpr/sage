package sage.client.internal

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantLock
import javax.net.ssl.SSLException

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Bytes, CommandSpan, Message, Outcome, PatternMessage, SageEvent, SageException}
import sage.SageException.*
import sage.client.{AuthConfig, BackoffConfig, DedicatedPoolConfig, Endpoint, PubSubConfig, ReadFrom, SageConfig, Topology, WatchdogConfig}
import sage.cluster.Node
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*
import sage.protocol.Frame

/**
  * The command surface shared by [[Client]] and [[TransactionScope]]: every command as a concrete method delegating to [[run]], so anything
  * implementing `run` — a fake, or a backend adapter lowering `F` to its native effect — gets the whole catalogue.
  */
trait CommandRunner[F[_], K](using KeyCodec[K]) {

  /**
    * Runs a [[sage.commands.Command]] and returns its decoded result. Every method on this trait is sugar over this one call.
    */
  def run[A](command: Command[A]): F[A]

  /**
    * Re-types this command surface to another key type, reusing the same connection — no new connection is opened. The runner is
    * key-agnostic (keys are encoded to bytes in the command builders before `run`), so this is a zero-cost view: `client.as[Array[Byte]]`
    * yields a command surface over binary keys, and it composes inside a transaction too (`tx.as[Array[Byte]].get(k)`).
    * [[Client]] and [[TransactionScope]] override this with a narrower return so the re-typed view keeps their full surface.
    */
  def as[K2](using KeyCodec[K2]): CommandRunner[F, K2] = {
    val self = this
    new CommandRunner[F, K2] {
      def run[A](command: Command[A]): F[A] = self.run(command)
    }
  }

  /**
    * Pings the server, returning `PONG` or the echoed `message` when one is given.
    */
  final def ping(message: Option[String] = None): F[String] = run(Connection.ping(message))

  /**
    * Appends `value` to the string at `key`, returning the new length.
    */
  final def append[V: ValueCodec](key: K, value: V): F[Long] = run(Strings.append(key, value))

  /**
    * Decrements the integer at `key` by one, returning the new value.
    */
  final def decr(key: K): F[Long] = run(Strings.decr(key))

  /**
    * Decrements the integer at `key` by `decrement`, returning the new value.
    */
  final def decrBy(key: K, decrement: Long): F[Long] = run(Strings.decrBy(key, decrement))

  /**
    * Gets the value at `key`, or `None` if it does not exist.
    */
  final def get[V: ValueCodec](key: K): F[Option[V]] = run(Strings.get(key))

  /**
    * Gets the value at `key` and deletes it atomically, returning the value (or `None`).
    */
  final def getDel[V: ValueCodec](key: K): F[Option[V]] = run(Strings.getDel(key))

  /**
    * Gets the value at `key`, optionally updating its expiry per `expiry`.
    */
  final def getEx[V: ValueCodec](key: K, expiry: GetExpiry = GetExpiry.Keep): F[Option[V]] =
    run(Strings.getEx(key, expiry))

  /**
    * Returns the substring of the value at `key` between `start` and `end`, inclusive; negative indices count from the end.
    */
  final def getRange[V: ValueCodec](key: K, start: Long, end: Long): F[V] = run(Strings.getRange(key, start, end))

  /**
    * Increments the integer at `key` by one, returning the new value.
    */
  final def incr(key: K): F[Long] = run(Strings.incr(key))

  /**
    * Increments the integer at `key` by `increment`, returning the new value.
    */
  final def incrBy(key: K, increment: Long): F[Long] = run(Strings.incrBy(key, increment))

  /**
    * Increments the floating-point value at `key` by `increment`, returning the new value.
    */
  final def incrByFloat(key: K, increment: Double): F[Double] = run(Strings.incrByFloat(key, increment))

  /**
    * Gets the values at several keys in request order, each `None` if that key is absent. In cluster mode, keys spanning slots are
    * transparently split into one MGET per exact slot and merged; each subgroup is atomic, but the combined result is not a cluster-wide
    * point-in-time snapshot. Cross-slot MGET remains invalid inside a transaction.
    */
  final def mGet[V: ValueCodec](first: K, rest: K*): F[Vector[Option[V]]] = run(Strings.mGet(first, rest*))

  /**
    * Sets several key/value pairs atomically on one server. In cluster mode, pairs spanning slots are set independently per exact slot; a
    * failed call may therefore have already set pairs in successful slot groups. Cross-slot MSET remains invalid inside a transaction.
    */
  final def mSet[V: ValueCodec](first: (K, V), rest: (K, V)*): F[Unit] = run(Strings.mSet(first, rest*))

  /**
    * Sets several key/value pairs only if none of the keys already exist, returning whether it applied.
    */
  final def mSetNx[V: ValueCodec](first: (K, V), rest: (K, V)*): F[Boolean] = run(Strings.mSetNx(first, rest*))

  /**
    * Sets `key` to `value` with optional `expiry` and a set `condition`, returning whether the write was applied.
    */
  final def set[V: ValueCodec](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  ): F[Boolean] = run(Strings.set(key, value, expiry, condition))

  /**
    * Like [[set]], but returns the previous value (`SET … GET`).
    */
  final def setGet[V: ValueCodec](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  ): F[Option[V]] = run(Strings.setGet(key, value, expiry, condition))

  /**
    * Overwrites part of the string at `key` starting at `offset`, zero-padding if needed, returning the new length.
    */
  final def setRange[V: ValueCodec](key: K, offset: Long, value: V): F[Long] = run(Strings.setRange(key, offset, value))

  /**
    * Returns the length of the string at `key`, or 0 if it is absent.
    */
  final def strLen(key: K): F[Long] = run(Strings.strLen(key))

  /**
    * Returns the longest common subsequence of the strings at `key1` and `key2`.
    */
  final def lcs[V: ValueCodec](key1: K, key2: K): F[V] = run(Strings.lcs(key1, key2))

  /**
    * Returns the length of the longest common subsequence of the strings at `key1` and `key2`.
    */
  final def lcsLen(key1: K, key2: K): F[Long] = run(Strings.lcsLen(key1, key2))

  /**
    * Returns the matched ranges of the longest common subsequence; see [[sage.commands.LcsMatches]].
    */
  final def lcsIdx(key1: K, key2: K, minMatchLen: Option[Long] = None, withMatchLen: Boolean = false): F[LcsMatches] =
    run(Strings.lcsIdx(key1, key2, minMatchLen, withMatchLen))

  /**
    * Returns a digest of the value at `key` (Valkey `DIGEST`), or `None` if the key is absent.
    */
  final def digest(key: K): F[Option[String]] = run(Strings.digest(key))

  /**
    * Deletes `key` only if `condition` holds against its current value (Valkey `DELEX`), returning whether it was deleted.
    */
  final def delex[V: ValueCodec](key: K, condition: DelexCondition[V] = DelexCondition.Always): F[Boolean] =
    run(Strings.delex(key, condition))

  /**
    * Atomically sets several key/value pairs with a shared `expiry` and set `condition` (Valkey `MSETEX`), returning whether applied.
    */
  final def msetEx[V: ValueCodec](
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  )(first: (K, V), rest: (K, V)*): F[Boolean] = run(Strings.msetEx(expiry, condition)(first, rest*))

  /**
    * Increments the integer at `key` (Valkey `INCREX`), optionally clamping to bounds and updating expiry; see [[sage.commands.IncrExResult]].
    */
  final def increxBy(
    key: K,
    increment: Long = 1,
    saturate: Boolean = false,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  ): F[IncrExResult[Long]] = run(Strings.increxBy(key, increment, saturate, lowerBound, upperBound, expiry))

  /**
    * The floating-point form of [[increxBy]] (Valkey `INCREX`).
    */
  final def increxByFloat(
    key: K,
    increment: Double,
    saturate: Boolean = false,
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  ): F[IncrExResult[Double]] = run(Strings.increxByFloat(key, increment, saturate, lowerBound, upperBound, expiry))

  /**
    * Copies the value at `source` to `destination`; `replace` overwrites an existing destination. Returns whether it copied.
    */
  final def copy(source: K, destination: K, replace: Boolean = false): F[Boolean] =
    run(Keys.copy(source, destination, replace))

  /**
    * Deletes the given keys, returning how many existed. In cluster mode, keys spanning slots are deleted independently per exact slot and
    * the counts are summed; a failed call may therefore have already deleted keys in successful slot groups.
    */
  final def del(first: K, rest: K*): F[Long] = run(Keys.del(first, rest*))

  /**
    * Deletes `key` only if its current value equals `value` (Valkey `DELIFEQ`), returning whether it was deleted.
    */
  final def delIfEq[V: ValueCodec](key: K, value: V): F[Boolean] = run(Keys.delIfEq(key, value))

  /**
    * Returns how many of the given keys exist (counting duplicates). In cluster mode, keys spanning slots are checked independently per exact
    * slot and the counts are summed.
    */
  final def exists(first: K, rest: K*): F[Long] = run(Keys.exists(first, rest*))

  /**
    * Sets a TTL of `in` on `key`, guarded by `condition`, returning whether the expiry was set.
    */
  final def expire(key: K, in: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always): F[Boolean] =
    run(Keys.expire(key, in, condition))

  /**
    * Sets `key` to expire at the absolute instant `at`, guarded by `condition`, returning whether the expiry was set.
    */
  final def expireAt(key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always): F[Boolean] =
    run(Keys.expireAt(key, at, condition))

  /**
    * Returns when `key` expires; see [[sage.commands.ExpiryTime]].
    */
  final def expireTime(key: K): F[ExpiryTime] = run(Keys.expireTime(key))

  /**
    * Like [[expireTime]], in milliseconds (`PEXPIRETIME`).
    */
  final def pExpireTime(key: K): F[ExpiryTime] = run(Keys.pExpireTime(key))

  /**
    * Returns all keys matching the glob `pattern`. O(n) over the keyspace — prefer [[scan]] on large databases.
    */
  final def keys(pattern: String): F[Vector[K]] = run(Keys.keys(pattern))

  /**
    * Removes any expiry from `key`, returning whether one was removed.
    */
  final def persist(key: K): F[Boolean] = run(Keys.persist(key))

  /**
    * Returns the remaining TTL of `key` in millisecond precision; see [[sage.commands.Ttl]].
    */
  final def pTtl(key: K): F[Ttl] = run(Keys.pTtl(key))

  /**
    * Returns a random key from the current database, or `None` if it is empty.
    */
  final def randomKey: F[Option[K]] = run(Keys.randomKey)

  /**
    * Renames `source` to `destination`, overwriting it if it exists.
    */
  final def rename(source: K, destination: K): F[Unit] = run(Keys.rename(source, destination))

  /**
    * Renames `source` to `destination` only if `destination` does not exist, returning whether it renamed.
    */
  final def renameNx(source: K, destination: K): F[Boolean] = run(Keys.renameNx(source, destination))

  /**
    * One `SCAN` page over the keyspace from `cursor`, optionally filtered by `pattern`/`ofType` with a `count` hint; see [[sage.commands.ScanPage]].
    */
  final def scan(
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): F[ScanPage[K]] = run(Keys.scan(cursor, pattern, count, ofType))

  /**
    * Updates the last-access time of the given keys without reading them, returning how many existed. In cluster mode, keys spanning slots
    * are touched independently per exact slot and the counts are summed.
    */
  final def touch(first: K, rest: K*): F[Long] = run(Keys.touch(first, rest*))

  /**
    * Returns the remaining TTL of `key` in second precision; see [[sage.commands.Ttl]].
    */
  final def ttl(key: K): F[Ttl] = run(Keys.ttl(key))

  /**
    * Returns the data type held at `key`, or `None` if it is absent; see [[sage.commands.RedisType]].
    */
  final def typeOf(key: K): F[Option[RedisType]] = run(Keys.typeOf(key))

  /**
    * Deletes the given keys, reclaiming memory asynchronously, returning how many existed. In cluster mode, keys spanning slots are unlinked
    * independently per exact slot and the counts are summed; a failed call may therefore have already unlinked keys in successful slot groups.
    */
  final def unlink(first: K, rest: K*): F[Long] = run(Keys.unlink(first, rest*))

  /**
    * Sorts the list/set/sorted-set at `key`, optionally by an external pattern and projecting via `get`; see [[sage.commands.SortOrder]].
    */
  final def sort[V: ValueCodec](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Vector[Option[V]]] = run(Keys.sort(key, by, limit, get, order, alpha))

  /**
    * Like [[sort]], but stores the result in `destination` and returns its length.
    */
  final def sortStore(
    destination: K,
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Long] = run(Keys.sortStore(destination, key, by, limit, get, order, alpha))

  /**
    * The read-only form of [[sort]] (`SORT_RO`), eligible for replica routing.
    */
  final def sortRo[V: ValueCodec](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Vector[Option[V]]] = run(Keys.sortRo(key, by, limit, get, order, alpha))

  /**
    * Moves `key` to database `db` on the same server, returning whether it was moved. Not available on a cluster.
    */
  final def move(key: K, db: Int): F[Boolean] = run(Keys.move(key, db))

  /**
    * Returns the serialized form of the value at `key` for use with [[restore]], or `None` if absent.
    */
  final def dump(key: K): F[Option[Bytes]] = run(Keys.dump(key))

  /**
    * Recreates `key` from a [[dump]] `payload`, with optional expiry/idle/freq metadata; `replace` overwrites an existing key.
    */
  final def restore(
    key: K,
    payload: Bytes,
    expiry: RestoreExpiry = RestoreExpiry.NoExpiry,
    replace: Boolean = false,
    idleTime: Option[FiniteDuration] = None,
    freq: Option[Long] = None
  ): F[Unit] = run(Keys.restore(key, payload, expiry, replace, idleTime, freq))

  /**
    * Transfers the given keys to another server, optionally keeping them locally (`copy`); see [[sage.commands.MigrateResult]].
    */
  final def migrate(
    host: String,
    port: Int,
    destinationDb: Int,
    timeout: FiniteDuration,
    copy: Boolean = false,
    replace: Boolean = false,
    auth: MigrateAuth = MigrateAuth.None
  )(first: K, rest: K*): F[MigrateResult] = run(Keys.migrate(host, port, destinationDb, timeout, copy, replace, auth)(first, rest*))

  /**
    * Returns the internal encoding of the value at `key` (`OBJECT ENCODING`), e.g. `listpack`/`skiplist`, or `None` if absent.
    */
  final def objectEncoding(key: K): F[Option[String]] = run(Keys.objectEncoding(key))

  /**
    * Returns the reference count of the value at `key` (`OBJECT REFCOUNT`), or `None` if absent.
    */
  final def objectRefCount(key: K): F[Option[Long]] = run(Keys.objectRefCount(key))

  /**
    * Returns the access frequency counter of `key` (`OBJECT FREQ`, requires an LFU eviction policy), or `None` if absent.
    */
  final def objectFreq(key: K): F[Option[Long]] = run(Keys.objectFreq(key))

  /**
    * Returns how long `key` has been idle (`OBJECT IDLETIME`), or `None` if absent.
    */
  final def objectIdleTime(key: K): F[Option[FiniteDuration]] = run(Keys.objectIdleTime(key))

  /**
    * Sets one or more fields in the hash at `key`, returning how many were newly added.
    */
  final def hSet[F0: KeyCodec, V: ValueCodec](key: K, first: (F0, V), rest: (F0, V)*): F[Long] =
    run(Hashes.hSet(key, first, rest*))

  /**
    * Sets `field` in the hash at `key` only if it does not already exist, returning whether it was set.
    */
  final def hSetNx[F0: KeyCodec, V: ValueCodec](key: K, field: F0, value: V): F[Boolean] =
    run(Hashes.hSetNx(key, field, value))

  /**
    * Gets the value of `field` in the hash at `key`, or `None` if the field or key is absent.
    */
  final def hGet[F0: KeyCodec, V: ValueCodec](key: K, field: F0): F[Option[V]] = run(Hashes.hGet(key, field))

  /**
    * Gets several fields' values from the hash at `key`, in request order, each `None` if absent.
    */
  final def hmGet[F0: KeyCodec, V: ValueCodec](key: K, first: F0, rest: F0*): F[Vector[Option[V]]] =
    run(Hashes.hmGet(key, first, rest*))

  /**
    * Deletes the given fields from the hash at `key`, returning how many existed.
    */
  final def hDel[F0: KeyCodec](key: K, first: F0, rest: F0*): F[Long] = run(Hashes.hDel(key, first, rest*))

  /**
    * Returns whether `field` exists in the hash at `key`.
    */
  final def hExists[F0: KeyCodec](key: K, field: F0): F[Boolean] = run(Hashes.hExists(key, field))

  /**
    * Returns the number of fields in the hash at `key`.
    */
  final def hLen(key: K): F[Long] = run(Hashes.hLen(key))

  /**
    * Returns the string length of `field`'s value in the hash at `key`, or 0 if absent.
    */
  final def hStrLen[F0: KeyCodec](key: K, field: F0): F[Long] = run(Hashes.hStrLen(key, field))

  /**
    * Returns all field names in the hash at `key`.
    */
  final def hKeys[F0: KeyCodec](key: K): F[Vector[F0]] = run(Hashes.hKeys(key))

  /**
    * Returns all values in the hash at `key`.
    */
  final def hVals[V: ValueCodec](key: K): F[Vector[V]] = run(Hashes.hVals(key))

  /**
    * Returns the whole hash at `key` as a map.
    */
  final def hGetAll[F0: KeyCodec, V: ValueCodec](key: K): F[Map[F0, V]] = run(Hashes.hGetAll(key))

  /**
    * Increments the integer field `field` in the hash at `key` by `increment`, returning the new value.
    */
  final def hIncrBy[F0: KeyCodec](key: K, field: F0, increment: Long): F[Long] =
    run(Hashes.hIncrBy(key, field, increment))

  /**
    * Increments the floating-point field `field` in the hash at `key` by `increment`, returning the new value.
    */
  final def hIncrByFloat[F0: KeyCodec](key: K, field: F0, increment: Double): F[Double] =
    run(Hashes.hIncrByFloat(key, field, increment))

  /**
    * Returns a random field name from the hash at `key`, or `None` if it is empty.
    */
  final def hRandField[F0: KeyCodec](key: K): F[Option[F0]] = run(Hashes.hRandField(key))

  /**
    * Returns `count` random field names from the hash at `key`; a negative `count` allows repeats.
    */
  final def hRandField[F0: KeyCodec](key: K, count: Long): F[Vector[F0]] = run(Hashes.hRandField(key, count))

  /**
    * Like [[hRandField]], but returns each field paired with its value.
    */
  final def hRandFieldWithValues[F0: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[(F0, V)]] =
    run(Hashes.hRandFieldWithValues(key, count))

  /**
    * One `HSCAN` page of field/value pairs over the hash at `key`; see [[sage.commands.ScanPage]].
    */
  final def hScan[F0: KeyCodec, V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[(F0, V)]] = run(Hashes.hScan(key, cursor, pattern, count))

  /**
    * Like [[hScan]], but returns field names only (`HSCAN … NOVALUES`).
    */
  final def hScanNoValues[F0: KeyCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[F0]] = run(Hashes.hScanNoValues(key, cursor, pattern, count))

  /**
    * Sets a TTL on the given hash fields, guarded by `condition`; one [[sage.commands.FieldExpiry]] per field, in request order.
    */
  final def hExpire[F0: KeyCodec](key: K, ttl: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(
    first: F0,
    rest: F0*
  ): F[Vector[FieldExpiry]] = run(Hashes.hExpire(key, ttl, condition)(first, rest*))

  /**
    * Like [[hExpire]], but at the absolute instant `at`.
    */
  final def hExpireAt[F0: KeyCodec](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(
    first: F0,
    rest: F0*
  ): F[Vector[FieldExpiry]] = run(Hashes.hExpireAt(key, at, condition)(first, rest*))

  /**
    * Returns when each given hash field expires; one [[sage.commands.FieldExpiryTime]] per field.
    */
  final def hExpireTime[F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldExpiryTime]] =
    run(Hashes.hExpireTime(key)(first, rest*))

  /**
    * Like [[hExpireTime]], in milliseconds (`HPEXPIRETIME`).
    */
  final def hpExpireTime[F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldExpiryTime]] =
    run(Hashes.hpExpireTime(key)(first, rest*))

  /**
    * Returns the remaining TTL of each given hash field in seconds; one [[sage.commands.FieldTtl]] per field.
    */
  final def hTtl[F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldTtl]] =
    run(Hashes.hTtl(key)(first, rest*))

  /**
    * Like [[hTtl]], in milliseconds (`HPTTL`).
    */
  final def hpTtl[F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldTtl]] =
    run(Hashes.hpTtl(key)(first, rest*))

  /**
    * Removes any expiry from the given hash fields; one [[sage.commands.FieldPersist]] per field.
    */
  final def hPersist[F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldPersist]] =
    run(Hashes.hPersist(key)(first, rest*))

  /**
    * Gets and deletes the given hash fields atomically (`HGETDEL`), each `None` if absent.
    */
  final def hGetDel[F0: KeyCodec, V: ValueCodec](key: K)(first: F0, rest: F0*): F[Vector[Option[V]]] =
    run(Hashes.hGetDel(key)(first, rest*))

  /**
    * Gets the given hash fields, optionally updating their expiry (`HGETEX`), each `None` if absent.
    */
  final def hGetEx[F0: KeyCodec, V: ValueCodec](key: K, expiry: GetExpiry = GetExpiry.Keep)(
    first: F0,
    rest: F0*
  ): F[Vector[Option[V]]] = run(Hashes.hGetEx(key, expiry)(first, rest*))

  /**
    * Sets hash fields with a shared expiry and a set `condition` (`HSETEX`), returning whether the write applied.
    */
  final def hSetEx[F0: KeyCodec, V: ValueCodec](
    key: K,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: HSetExCondition = HSetExCondition.Always
  )(first: (F0, V), rest: (F0, V)*): F[Boolean] = run(Hashes.hSetEx(key, expiry, condition)(first, rest*))

  /**
    * Prepends the given values to the list at `key` (left), returning the new length.
    */
  final def lPush[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.lPush(key, first, rest*))

  /**
    * Appends the given values to the list at `key` (right), returning the new length.
    */
  final def rPush[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.rPush(key, first, rest*))

  /**
    * Like [[lPush]], but only if the list already exists.
    */
  final def lPushX[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.lPushX(key, first, rest*))

  /**
    * Like [[rPush]], but only if the list already exists.
    */
  final def rPushX[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.rPushX(key, first, rest*))

  /**
    * Removes and returns the first (leftmost) element of the list at `key`, or `None` if empty.
    */
  final def lPop[V: ValueCodec](key: K): F[Option[V]] = run(Lists.lPop(key))

  /**
    * Removes and returns the last (rightmost) element of the list at `key`, or `None` if empty.
    */
  final def rPop[V: ValueCodec](key: K): F[Option[V]] = run(Lists.rPop(key))

  /**
    * Removes and returns up to `count` elements from the left of the list at `key`.
    */
  final def lPopCount[V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Lists.lPopCount(key, count))

  /**
    * Removes and returns up to `count` elements from the right of the list at `key`.
    */
  final def rPopCount[V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Lists.rPopCount(key, count))

  /**
    * Returns the length of the list at `key`.
    */
  final def lLen(key: K): F[Long] = run(Lists.lLen(key))

  /**
    * Returns the elements of the list at `key` between `start` and `stop`, inclusive; negative indices count from the end.
    */
  final def lRange[V: ValueCodec](key: K, start: Long, stop: Long): F[Vector[V]] = run(Lists.lRange(key, start, stop))

  /**
    * Returns the element at `index` in the list at `key` (negative counts from the end), or `None` if out of range.
    */
  final def lIndex[V: ValueCodec](key: K, index: Long): F[Option[V]] = run(Lists.lIndex(key, index))

  /**
    * Sets the element at `index` in the list at `key`; fails if the index is out of range.
    */
  final def lSet[V: ValueCodec](key: K, index: Long, value: V): F[Unit] = run(Lists.lSet(key, index, value))

  /**
    * Inserts `value` before or after the first occurrence of `pivot` in the list at `key`; returns the new length, or -1 if `pivot` is absent.
    */
  final def lInsert[V: ValueCodec](key: K, position: InsertPosition, pivot: V, value: V): F[Long] =
    run(Lists.lInsert(key, position, pivot, value))

  /**
    * Removes occurrences of `value` from the list at `key`; `count` > 0 from head, < 0 from tail, 0 all. Returns how many were removed.
    */
  final def lRem[V: ValueCodec](key: K, count: Long, value: V): F[Long] = run(Lists.lRem(key, count, value))

  /**
    * Trims the list at `key` to the inclusive range `[start, stop]`, discarding the rest.
    */
  final def lTrim(key: K, start: Long, stop: Long): F[Unit] = run(Lists.lTrim(key, start, stop))

  /**
    * Returns the index of the first match of `element` in the list at `key`, honoring `rank`/`maxLen`, or `None`.
    */
  final def lPos[V: ValueCodec](key: K, element: V, rank: Option[Long] = None, maxLen: Option[Long] = None): F[Option[Long]] =
    run(Lists.lPos(key, element, rank, maxLen))

  /**
    * Like [[lPos]], but returns up to `count` matching indices (`count` 0 means all).
    */
  final def lPosCount[V: ValueCodec](
    key: K,
    element: V,
    count: Long,
    rank: Option[Long] = None,
    maxLen: Option[Long] = None
  ): F[Vector[Long]] = run(Lists.lPosCount(key, element, count, rank, maxLen))

  /**
    * Atomically pops an element from one end of `source` and pushes it to one end of `destination`, returning the moved element.
    */
  final def lMove[V: ValueCodec](source: K, destination: K, from: ListSide, to: ListSide): F[Option[V]] =
    run(Lists.lMove(source, destination, from, to))

  /**
    * Pops up to `count` elements from the given side of the first non-empty list among the keys, returning that key and the elements.
    */
  final def lMpop[V: ValueCodec](first: K, rest: K*)(side: ListSide, count: Option[Long] = None): F[Option[(K, Vector[V])]] =
    run(Lists.lMpop(first, rest*)(side, count))

  /**
    * Blocking [[lPop]] over several keys: waits up to `timeout` for an element on the left of any key.
    */
  final def blPop[V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V)]] =
    run(Lists.blPop(first, rest*)(timeout))

  /**
    * Blocking [[rPop]] over several keys: waits up to `timeout` for an element on the right of any key.
    */
  final def brPop[V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V)]] =
    run(Lists.brPop(first, rest*)(timeout))

  /**
    * Blocking [[lMove]]: waits up to `timeout` for an element in `source` to move.
    */
  final def blMove[V: ValueCodec](source: K, destination: K, from: ListSide, to: ListSide, timeout: BlockTimeout): F[Option[V]] =
    run(Lists.blMove(source, destination, from, to, timeout))

  /**
    * Blocking [[lMpop]]: waits up to `timeout` for an element on any of the keys.
    */
  final def blMpop[V: ValueCodec](first: K, rest: K*)(
    side: ListSide,
    timeout: BlockTimeout,
    count: Option[Long] = None
  ): F[Option[(K, Vector[V])]] = run(Lists.blMpop(first, rest*)(side, timeout, count))

  /**
    * Adds the given members to the set at `key`, returning how many were newly added.
    */
  final def sAdd[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Sets.sAdd(key, first, rest*))

  /**
    * Removes the given members from the set at `key`, returning how many were present.
    */
  final def sRem[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Sets.sRem(key, first, rest*))

  /**
    * Returns the number of members in the set at `key`.
    */
  final def sCard(key: K): F[Long] = run(Sets.sCard(key))

  /**
    * Returns whether `member` is in the set at `key`.
    */
  final def sIsMember[V: ValueCodec](key: K, member: V): F[Boolean] = run(Sets.sIsMember(key, member))

  /**
    * Returns, for each given member, whether it is in the set at `key`, in request order.
    */
  final def sMisMember[V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Boolean]] =
    run(Sets.sMisMember(key, first, rest*))

  /**
    * Returns all members of the set at `key`.
    */
  final def sMembers[V: ValueCodec](key: K): F[Set[V]] = run(Sets.sMembers(key))

  /**
    * Atomically moves `member` from the set at `source` to the set at `destination`, returning whether it was present.
    */
  final def sMove[V: ValueCodec](source: K, destination: K, member: V): F[Boolean] =
    run(Sets.sMove(source, destination, member))

  /**
    * Removes and returns one random member of the set at `key`, or `None` if empty.
    */
  final def sPop[V: ValueCodec](key: K): F[Option[V]] = run(Sets.sPop(key))

  /**
    * Removes and returns up to `count` random members of the set at `key`.
    */
  final def sPopCount[V: ValueCodec](key: K, count: Long): F[Set[V]] = run(Sets.sPopCount(key, count))

  /**
    * Returns one random member of the set at `key` without removing it, or `None` if empty.
    */
  final def sRandMember[V: ValueCodec](key: K): F[Option[V]] = run(Sets.sRandMember(key))

  /**
    * Returns `count` random members of the set at `key` without removing them; a negative `count` allows repeats.
    */
  final def sRandMemberCount[V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Sets.sRandMemberCount(key, count))

  /**
    * Returns the members in the first set that are not in any of the others.
    */
  final def sDiff[V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sDiff(first, rest*))

  /**
    * Like [[sDiff]], but stores the result in `destination` and returns its cardinality.
    */
  final def sDiffStore(destination: K, first: K, rest: K*): F[Long] = run(Sets.sDiffStore(destination, first, rest*))

  /**
    * Returns the members common to all the given sets.
    */
  final def sInter[V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sInter(first, rest*))

  /**
    * Like [[sInter]], but stores the result in `destination` and returns its cardinality.
    */
  final def sInterStore(destination: K, first: K, rest: K*): F[Long] = run(Sets.sInterStore(destination, first, rest*))

  /**
    * Returns the cardinality of the intersection of the given sets, capped at `limit` if set (`SINTERCARD`).
    */
  final def sInterCard(first: K, rest: K*)(limit: Option[Long] = None): F[Long] = run(Sets.sInterCard(first, rest*)(limit))

  /**
    * Returns the union of all the given sets.
    */
  final def sUnion[V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sUnion(first, rest*))

  /**
    * Like [[sUnion]], but stores the result in `destination` and returns its cardinality.
    */
  final def sUnionStore(destination: K, first: K, rest: K*): F[Long] = run(Sets.sUnionStore(destination, first, rest*))

  /**
    * One `SSCAN` page over the members of the set at `key`; see [[sage.commands.ScanPage]].
    */
  final def sScan[V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[V]] = run(Sets.sScan(key, cursor, pattern, count))

  /**
    * Adds the given members with scores to the sorted set at `key`, guarded by `condition`; returns how many were added (or changed, if `changed`).
    */
  final def zAdd[V: ValueCodec](key: K, condition: ZAddCondition = ZAddCondition.Always, changed: Boolean = false)(
    first: (V, Double),
    rest: (V, Double)*
  ): F[Long] = run(SortedSets.zAdd(key, condition, changed)(first, rest*))

  /**
    * Adds or updates one member and returns its new score (`ZADD … INCR`), or `None` when `condition` blocks the write.
    */
  final def zAddIncr[V: ValueCodec](
    key: K,
    condition: ZAddCondition = ZAddCondition.Always
  )(member: V, score: Double): F[Option[Double]] =
    run(SortedSets.zAddIncr(key, condition)(member, score))

  /**
    * Returns the number of members in the sorted set at `key`.
    */
  final def zCard(key: K): F[Long] = run(SortedSets.zCard(key))

  /**
    * Returns the score of `member` in the sorted set at `key`, or `None` if absent.
    */
  final def zScore[V: ValueCodec](key: K, member: V): F[Option[Double]] = run(SortedSets.zScore(key, member))

  /**
    * Returns the scores of several members in request order, each `None` if absent.
    */
  final def zMScore[V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[Double]]] =
    run(SortedSets.zMScore(key, first, rest*))

  /**
    * Increments the score of `member` in the sorted set at `key` by `increment`, returning the new score.
    */
  final def zIncrBy[V: ValueCodec](key: K, member: V, increment: Double): F[Double] =
    run(SortedSets.zIncrBy(key, member, increment))

  /**
    * Returns the rank of `member` (0-based, lowest score first), or `None` if absent.
    */
  final def zRank[V: ValueCodec](key: K, member: V): F[Option[Long]] = run(SortedSets.zRank(key, member))

  /**
    * Like [[zRank]], but also returns the member's score.
    */
  final def zRankWithScore[V: ValueCodec](key: K, member: V): F[Option[(Long, Double)]] =
    run(SortedSets.zRankWithScore(key, member))

  /**
    * Returns the rank of `member` counting from the highest score (`ZREVRANK`), or `None` if absent.
    */
  final def zRevRank[V: ValueCodec](key: K, member: V): F[Option[Long]] = run(SortedSets.zRevRank(key, member))

  /**
    * Like [[zRevRank]], but also returns the member's score.
    */
  final def zRevRankWithScore[V: ValueCodec](key: K, member: V): F[Option[(Long, Double)]] =
    run(SortedSets.zRevRankWithScore(key, member))

  /**
    * Counts members of the sorted set at `key` whose score is within `[min, max]`; see [[sage.commands.ScoreBoundary]].
    */
  final def zCount(key: K, min: ScoreBoundary, max: ScoreBoundary): F[Long] = run(SortedSets.zCount(key, min, max))

  /**
    * Counts members within a lexicographic range (`ZLEXCOUNT`); meaningful only when all members share the same score.
    */
  final def zLexCount[V: ValueCodec](key: K, min: LexBoundary[V], max: LexBoundary[V]): F[Long] =
    run(SortedSets.zLexCount(key, min, max))

  /**
    * Returns members of the sorted set at `key` for the given range (by index, score, or lex); see [[sage.commands.ZRange]].
    */
  final def zRange[V: ValueCodec](key: K, range: ZRange[V]): F[Vector[V]] = run(SortedSets.zRange(key, range))

  /**
    * Like [[zRange]], but returns each member paired with its score.
    */
  final def zRangeWithScores[V: ValueCodec](key: K, range: ZRange[V]): F[Vector[(V, Double)]] =
    run(SortedSets.zRangeWithScores(key, range))

  /**
    * Like [[zRange]], but stores the selected members in `destination` and returns its cardinality.
    */
  final def zRangeStore[V: ValueCodec](destination: K, source: K, range: ZRange[V]): F[Long] =
    run(SortedSets.zRangeStore(destination, source, range))

  /**
    * Removes the given members from the sorted set at `key`, returning how many were present.
    */
  final def zRem[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(SortedSets.zRem(key, first, rest*))

  /**
    * Removes members of the sorted set at `key` whose rank is within `[start, stop]`, returning how many were removed.
    */
  final def zRemRangeByRank(key: K, start: Long, stop: Long): F[Long] = run(SortedSets.zRemRangeByRank(key, start, stop))

  /**
    * Removes members whose score is within `[min, max]`, returning how many were removed.
    */
  final def zRemRangeByScore(key: K, min: ScoreBoundary, max: ScoreBoundary): F[Long] =
    run(SortedSets.zRemRangeByScore(key, min, max))

  /**
    * Removes members within a lexicographic range, returning how many were removed.
    */
  final def zRemRangeByLex[V: ValueCodec](key: K, min: LexBoundary[V], max: LexBoundary[V]): F[Long] =
    run(SortedSets.zRemRangeByLex(key, min, max))

  /**
    * Removes and returns the lowest-scored member of the sorted set at `key`, or `None` if empty.
    */
  final def zPopMin[V: ValueCodec](key: K): F[Option[(V, Double)]] = run(SortedSets.zPopMin(key))

  /**
    * Removes and returns the highest-scored member of the sorted set at `key`, or `None` if empty.
    */
  final def zPopMax[V: ValueCodec](key: K): F[Option[(V, Double)]] = run(SortedSets.zPopMax(key))

  /**
    * Removes and returns up to `count` lowest-scored members of the sorted set at `key`.
    */
  final def zPopMinCount[V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] = run(SortedSets.zPopMinCount(key, count))

  /**
    * Removes and returns up to `count` highest-scored members of the sorted set at `key`.
    */
  final def zPopMaxCount[V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] = run(SortedSets.zPopMaxCount(key, count))

  /**
    * Pops up to `count` members from the min or max end of the first non-empty sorted set among the keys; see [[sage.commands.MinMax]].
    */
  final def zMpop[V: ValueCodec](first: K, rest: K*)(minMax: MinMax, count: Option[Long] = None): F[Option[(K, Vector[(V, Double)])]] =
    run(SortedSets.zMpop(first, rest*)(minMax, count))

  /**
    * Blocking [[zPopMin]] over several keys: waits up to `timeout` for the lowest-scored member of any key.
    */
  final def bzPopMin[V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V, Double)]] =
    run(SortedSets.bzPopMin(first, rest*)(timeout))

  /**
    * Blocking [[zPopMax]] over several keys: waits up to `timeout` for the highest-scored member of any key.
    */
  final def bzPopMax[V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V, Double)]] =
    run(SortedSets.bzPopMax(first, rest*)(timeout))

  /**
    * Blocking [[zMpop]]: waits up to `timeout` for a member on any of the keys.
    */
  final def bzMpop[V: ValueCodec](first: K, rest: K*)(
    minMax: MinMax,
    timeout: BlockTimeout,
    count: Option[Long] = None
  ): F[Option[(K, Vector[(V, Double)])]] = run(SortedSets.bzMpop(first, rest*)(minMax, timeout, count))

  /**
    * Returns one random member of the sorted set at `key` without removing it, or `None` if empty.
    */
  final def zRandMember[V: ValueCodec](key: K): F[Option[V]] = run(SortedSets.zRandMember(key))

  /**
    * Returns `count` random members of the sorted set at `key`; a negative `count` allows repeats.
    */
  final def zRandMemberCount[V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(SortedSets.zRandMemberCount(key, count))

  /**
    * Like [[zRandMemberCount]], but returns each member paired with its score.
    */
  final def zRandMemberWithScores[V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] =
    run(SortedSets.zRandMemberWithScores(key, count))

  /**
    * Returns the union of the given sorted sets, combining scores per `aggregate` (and optional `weights`); see [[sage.commands.Aggregate]].
    */
  final def zUnion[V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[V]] = run(SortedSets.zUnion(first, rest*)(weights, aggregate))

  /**
    * Like [[zUnion]], but returns each member paired with its combined score.
    */
  final def zUnionWithScores[V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[(V, Double)]] = run(SortedSets.zUnionWithScores(first, rest*)(weights, aggregate))

  /**
    * Like [[zUnion]], but stores the result in `destination` and returns its cardinality.
    */
  final def zUnionStore(destination: K, first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Long] = run(SortedSets.zUnionStore(destination, first, rest*)(weights, aggregate))

  /**
    * Returns the intersection of the given sorted sets, combining scores per `aggregate` (and optional `weights`).
    */
  final def zInter[V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[V]] = run(SortedSets.zInter(first, rest*)(weights, aggregate))

  /**
    * Like [[zInter]], but returns each member paired with its combined score.
    */
  final def zInterWithScores[V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[(V, Double)]] = run(SortedSets.zInterWithScores(first, rest*)(weights, aggregate))

  /**
    * Like [[zInter]], but stores the result in `destination` and returns its cardinality.
    */
  final def zInterStore(destination: K, first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Long] = run(SortedSets.zInterStore(destination, first, rest*)(weights, aggregate))

  /**
    * Returns the cardinality of the intersection of the given sorted sets, capped at `limit` if set (`ZINTERCARD`).
    */
  final def zInterCard(first: K, rest: K*)(limit: Option[Long] = None): F[Long] =
    run(SortedSets.zInterCard(first, rest*)(limit))

  /**
    * Returns the members of the first sorted set that are not in any of the others.
    */
  final def zDiff[V: ValueCodec](first: K, rest: K*): F[Vector[V]] = run(SortedSets.zDiff(first, rest*))

  /**
    * Like [[zDiff]], but returns each member paired with its score.
    */
  final def zDiffWithScores[V: ValueCodec](first: K, rest: K*): F[Vector[(V, Double)]] =
    run(SortedSets.zDiffWithScores(first, rest*))

  /**
    * Like [[zDiff]], but stores the result in `destination` and returns its cardinality.
    */
  final def zDiffStore(destination: K, first: K, rest: K*): F[Long] = run(SortedSets.zDiffStore(destination, first, rest*))

  /**
    * One `ZSCAN` page of member/score pairs over the sorted set at `key`; see [[sage.commands.ScanPage]].
    */
  final def zScan[V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[(V, Double)]] = run(SortedSets.zScan(key, cursor, pattern, count))

  /**
    * Publishes `message` to a classic channel, returning how many subscribers received it (across the cluster bus in cluster mode).
    */
  final def publish[V: ValueCodec](channel: String, message: V): F[Long] = run(Pubsub.publish(channel, message))

  /**
    * Publishes `message` to a Shard Channel, reaching only subscribers on the Node owning that channel's Slot.
    */
  final def sPublish[V: ValueCodec](channel: String, message: V): F[Long] = run(Pubsub.sPublish(channel, message))

  /**
    * Lists currently-active classic channels with at least one subscriber, optionally filtered by glob `pattern`.
    */
  final def pubsubChannels(pattern: Option[String] = None): F[Vector[String]] = run(Pubsub.pubsubChannels(pattern))

  /**
    * Lists currently-active Shard Channels, optionally filtered by glob `pattern`.
    */
  final def pubsubShardChannels(pattern: Option[String] = None): F[Vector[String]] = run(Pubsub.pubsubShardChannels(pattern))

  /**
    * Returns the subscriber count of each given classic channel.
    */
  final def pubsubNumSub(channels: String*): F[Map[String, Long]] = run(Pubsub.pubsubNumSub(channels*))

  /**
    * Returns the subscriber count of each given Shard Channel.
    */
  final def pubsubShardNumSub(channels: String*): F[Map[String, Long]] = run(Pubsub.pubsubShardNumSub(channels*))

  /**
    * Returns the number of active pattern subscriptions across all clients.
    */
  final def pubsubNumPat: F[Long] = run(Pubsub.pubsubNumPat)

  /**
    * Adds the given members at their coordinates to the geo set at `key`; returns how many were added (or changed, if `changed`).
    */
  final def geoAdd[V: ValueCodec](key: K, condition: GeoAddCondition = GeoAddCondition.Always, changed: Boolean = false)(
    first: (V, GeoCoordinates),
    rest: (V, GeoCoordinates)*
  ): F[Long] = run(Geo.geoAdd(key, condition, changed)(first, rest*))

  /**
    * Returns the distance between two members of the geo set at `key` in `unit`, or `None` if either is absent.
    */
  final def geoDist[V: ValueCodec](key: K, member1: V, member2: V, unit: GeoUnit = GeoUnit.Meters): F[Option[Double]] =
    run(Geo.geoDist(key, member1, member2, unit))

  /**
    * Returns the Geohash string of each given member, or `None` if absent.
    */
  final def geoHash[V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[String]]] =
    run(Geo.geoHash(key, first, rest*))

  /**
    * Returns the coordinates of each given member, or `None` if absent.
    */
  final def geoPos[V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[GeoCoordinates]]] =
    run(Geo.geoPos(key, first, rest*))

  /**
    * Returns members within `shape` of `origin`; see [[sage.commands.GeoOrigin]]/[[sage.commands.GeoShape]].
    */
  final def geoSearch[V: ValueCodec](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  ): F[Vector[V]] = run(Geo.geoSearch(key, origin, shape, sort, count))

  /**
    * Like [[geoSearch]], but also returns the requested projections (distance/coordinates/hash); see [[sage.commands.GeoSearchResult]].
    */
  final def geoSearchWith[V: ValueCodec](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    withCoord: Boolean = false,
    withDist: Boolean = false,
    withHash: Boolean = false,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  ): F[Vector[GeoSearchResult[V]]] = run(Geo.geoSearchWith(key, origin, shape, withCoord, withDist, withHash, sort, count))

  /**
    * Like [[geoSearch]], but stores the matches in `destination` (or their distances, if `storeDist`) and returns the count.
    */
  final def geoSearchStore[V: ValueCodec](
    destination: K,
    source: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None,
    storeDist: Boolean = false
  ): F[Long] = run(Geo.geoSearchStore(destination, source, origin, shape, sort, count, storeDist))

  /**
    * Sets the bit at `offset` in the string at `key`, returning its previous value.
    */
  final def setBit(key: K, offset: Long, value: Boolean): F[Boolean] = run(Bitmaps.setBit(key, offset, value))

  /**
    * Returns the bit at `offset` in the string at `key`.
    */
  final def getBit(key: K, offset: Long): F[Boolean] = run(Bitmaps.getBit(key, offset))

  /**
    * Counts the set bits in the string at `key`, optionally within a [[sage.commands.BitRange]].
    */
  final def bitCount(key: K, range: Option[BitRange] = None): F[Long] = run(Bitmaps.bitCount(key, range))

  /**
    * Returns the position of the first `bit` (set/clear) in the string at `key`, optionally within a [[sage.commands.BitPosRange]].
    */
  final def bitPos(key: K, bit: Boolean, range: Option[BitPosRange] = None): F[Long] = run(Bitmaps.bitPos(key, bit, range))

  /**
    * Stores the bitwise AND of the source keys into `destination`, returning the result's byte length.
    */
  final def bitOpAnd(destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpAnd(destination, first, rest*))

  /**
    * Stores the bitwise OR of the source keys into `destination`, returning the result's byte length.
    */
  final def bitOpOr(destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpOr(destination, first, rest*))

  /**
    * Stores the bitwise XOR of the source keys into `destination`, returning the result's byte length.
    */
  final def bitOpXor(destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpXor(destination, first, rest*))

  /**
    * Stores the bitwise NOT of `source` into `destination`, returning the result's byte length.
    */
  final def bitOpNot(destination: K, source: K): F[Long] = run(Bitmaps.bitOpNot(destination, source))

  /**
    * Runs a sequence of bitfield operations on `key`; one result per non-`Overflow` op. See [[sage.commands.BitFieldOp]].
    */
  final def bitField(key: K, first: BitFieldOp, rest: BitFieldOp*): F[Vector[Option[Long]]] =
    run(Bitmaps.bitField(key, first, rest*))

  /**
    * The read-only form of [[bitField]] (`BITFIELD_RO`), accepting only `Get` operations.
    */
  final def bitFieldRo(key: K, first: BitFieldOp.Get, rest: BitFieldOp.Get*): F[Vector[Long]] =
    run(Bitmaps.bitFieldRo(key, first, rest*))

  /**
    * Adds elements to the HyperLogLog at `key`, returning whether its estimate likely changed.
    */
  final def pfAdd[V: ValueCodec](key: K, elements: V*): F[Boolean] = run(HyperLogLog.pfAdd(key, elements*))

  /**
    * Returns the estimated cardinality of the union of the given HyperLogLogs.
    */
  final def pfCount(first: K, rest: K*): F[Long] = run(HyperLogLog.pfCount(first, rest*))

  /**
    * Merges the source HyperLogLogs into `destination`.
    */
  final def pfMerge(destination: K, sources: K*): F[Unit] = run(HyperLogLog.pfMerge(destination, sources*))

  /**
    * Appends an entry of field/value pairs to the Stream at `key`, optionally trimming; returns the assigned ID. See [[sage.commands.XAddId]].
    */
  final def xAdd[F0: KeyCodec, V: ValueCodec](
    key: K,
    id: XAddId = XAddId.Auto,
    trim: Option[Trimming] = None,
    policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef
  )(first: (F0, V), rest: (F0, V)*): F[StreamId] = run(Streams.xAdd(key, id, trim, policy)(first, rest*))

  /**
    * Like [[xAdd]], but does not create the Stream if it is absent (`XADD … NOMKSTREAM`); returns `None` in that case.
    */
  final def xAddNoMkStream[F0: KeyCodec, V: ValueCodec](
    key: K,
    id: XAddId = XAddId.Auto,
    trim: Option[Trimming] = None,
    policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef
  )(first: (F0, V), rest: (F0, V)*): F[Option[StreamId]] = run(Streams.xAddNoMkStream(key, id, trim, policy)(first, rest*))

  /**
    * Returns the number of entries in the Stream at `key`.
    */
  final def xLen(key: K): F[Long] = run(Streams.xLen(key))

  /**
    * Deletes the given entry IDs from the Stream at `key`, returning how many were removed.
    */
  final def xDel(key: K)(first: StreamId, rest: StreamId*): F[Long] = run(Streams.xDel(key)(first, rest*))

  /**
    * Trims the Stream at `key` per `trim` (by max length or min ID); returns how many entries were removed. See [[sage.commands.Trimming]].
    */
  final def xTrim(key: K, trim: Trimming, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef): F[Long] =
    run(Streams.xTrim(key, trim, policy))

  /**
    * Sets the Stream's last-generated ID and optional bookkeeping counters (`XSETID`).
    */
  final def xSetId(key: K, id: GroupStartId, entriesAdded: Option[Long] = None, maxDeletedId: Option[StreamId] = None): F[Unit] =
    run(Streams.xSetId(key, id, entriesAdded, maxDeletedId))

  /**
    * Configures the Stream's idempotency window (`XCFGSET`).
    */
  final def xCfgSet(key: K, idmpDuration: Option[FiniteDuration] = None, idmpMaxSize: Option[Long] = None): F[Unit] =
    run(Streams.xCfgSet(key, idmpDuration, idmpMaxSize))

  /**
    * Returns Stream entries in the ID range `[start, end]`, up to `count`; see [[sage.commands.StreamRangeId]]/[[sage.commands.StreamEntry]].
    */
  final def xRange[F0: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    count: Option[Long] = None
  ): F[Vector[StreamEntry[F0, V]]] = run(Streams.xRange(key, start, end, count))

  /**
    * Like [[xRange]], but in reverse order from `end` down to `start`.
    */
  final def xRevRange[F0: KeyCodec, V: ValueCodec](
    key: K,
    end: StreamRangeId = StreamRangeId.Max,
    start: StreamRangeId = StreamRangeId.Min,
    count: Option[Long] = None
  ): F[Vector[StreamEntry[F0, V]]] = run(Streams.xRevRange(key, end, start, count))

  /**
    * Reads new entries from one or more Streams past each given [[sage.commands.ReadId]], optionally blocking up to `block`.
    */
  final def xRead[F0: KeyCodec, V: ValueCodec](first: (K, ReadId), rest: (K, ReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None
  ): F[Vector[(K, Vector[StreamEntry[F0, V]])]] = run(Streams.xRead(first, rest*)(count, block))

  /**
    * Reads entries for a Consumer Group as `consumer`, advancing the group on `>` or re-reading pending history otherwise; see [[sage.commands.GroupReadId]].
    */
  final def xReadGroup[F0: KeyCodec, V: ValueCodec](group: String, consumer: String)(first: (K, GroupReadId), rest: (K, GroupReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None,
    noAck: Boolean = false
  ): F[Vector[(K, Vector[StreamEntry[F0, V]])]] = run(Streams.xReadGroup(group, consumer)(first, rest*)(count, block, noAck))

  /**
    * Acknowledges the given entry IDs for a Consumer Group, removing them from its Pending Entries List; returns how many were acknowledged.
    */
  final def xAck(key: K, group: String)(first: StreamId, rest: StreamId*): F[Long] = run(Streams.xAck(key, group)(first, rest*))

  /**
    * Creates a Consumer Group on the Stream at `key`, starting from `id`; `mkStream` creates the Stream if absent.
    */
  final def xGroupCreate(
    key: K,
    group: String,
    id: GroupStartId = GroupStartId.Last,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): F[Unit] = run(Streams.xGroupCreate(key, group, id, mkStream, entriesRead))

  /**
    * Repositions a Consumer Group's last-delivered ID.
    */
  final def xGroupSetId(key: K, group: String, id: GroupStartId = GroupStartId.Last, entriesRead: Option[Long] = None): F[Unit] =
    run(Streams.xGroupSetId(key, group, id, entriesRead))

  /**
    * Destroys a Consumer Group and its pending state, returning whether it existed.
    */
  final def xGroupDestroy(key: K, group: String): F[Boolean] = run(Streams.xGroupDestroy(key, group))

  /**
    * Explicitly creates a Consumer in a group, returning whether it was newly created.
    */
  final def xGroupCreateConsumer(key: K, group: String, consumer: String): F[Boolean] =
    run(Streams.xGroupCreateConsumer(key, group, consumer))

  /**
    * Removes a Consumer from a group, returning how many pending entries it still owned.
    */
  final def xGroupDelConsumer(key: K, group: String, consumer: String): F[Long] =
    run(Streams.xGroupDelConsumer(key, group, consumer))

  /**
    * Transfers ownership of the given pending entries to `consumer`, if idle at least `minIdle`; returns the claimed entries.
    */
  final def xClaim[F0: KeyCodec, V: ValueCodec](key: K, group: String, consumer: String, minIdle: FiniteDuration)(
    first: StreamId,
    rest: StreamId*
  )(idle: Option[ClaimIdle] = None, retryCount: Option[Long] = None, force: Boolean = false): F[Vector[StreamEntry[F0, V]]] =
    run(Streams.xClaim(key, group, consumer, minIdle)(first, rest*)(idle, retryCount, force))

  /**
    * Like [[xClaim]], but returns only the claimed entry IDs (`XCLAIM … JUSTID`).
    */
  final def xClaimJustId(key: K, group: String, consumer: String, minIdle: FiniteDuration)(first: StreamId, rest: StreamId*)(
    idle: Option[ClaimIdle] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): F[Vector[StreamId]] = run(Streams.xClaimJustId(key, group, consumer, minIdle)(first, rest*)(idle, retryCount, force))

  /**
    * Scans a group's pending entries from `start`, claiming idle ones for `consumer`; see [[sage.commands.XAutoClaimResult]].
    */
  final def xAutoClaim[F0: KeyCodec, V: ValueCodec](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  ): F[XAutoClaimResult[F0, V]] = run(Streams.xAutoClaim(key, group, consumer, minIdle, start, count))

  /**
    * Like [[xAutoClaim]], but returns only the claimed entry IDs; see [[sage.commands.XAutoClaimJustIdResult]].
    */
  final def xAutoClaimJustId(
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  ): F[XAutoClaimJustIdResult] = run(Streams.xAutoClaimJustId(key, group, consumer, minIdle, start, count))

  /**
    * Returns a group-level summary of the Pending Entries List; see [[sage.commands.PendingSummary]].
    */
  final def xPending(key: K, group: String): F[PendingSummary] = run(Streams.xPending(key, group))

  /**
    * Returns per-entry pending detail over an ID range, optionally filtered by `consumer`/`idle`; see [[sage.commands.PendingEntry]].
    */
  final def xPendingExtended(
    key: K,
    group: String,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    count: Long = 10L,
    consumer: Option[String] = None,
    idle: Option[FiniteDuration] = None
  ): F[Vector[PendingEntry]] = run(Streams.xPendingExtended(key, group, start, end, count, consumer, idle))

  /**
    * Returns summary metadata about the Stream at `key`; see [[sage.commands.StreamInfo]].
    */
  final def xInfoStream[F0: KeyCodec, V: ValueCodec](key: K): F[StreamInfo[F0, V]] = run(StreamInfo.xInfoStream(key))

  /**
    * Returns the full Stream introspection, including groups and PEL entries; see [[sage.commands.StreamInfoFull]].
    */
  final def xInfoStreamFull[F0: KeyCodec, V: ValueCodec](key: K, count: Option[Long] = None): F[StreamInfoFull[F0, V]] =
    run(StreamInfo.xInfoStreamFull(key, count))

  /**
    * Returns one entry per Consumer Group on the Stream at `key`; see [[sage.commands.GroupInfo]].
    */
  final def xInfoGroups(key: K): F[Vector[GroupInfo]] = run(StreamInfo.xInfoGroups(key))

  /**
    * Returns one entry per Consumer in a group; see [[sage.commands.ConsumerInfo]].
    */
  final def xInfoConsumers(key: K, group: String): F[Vector[ConsumerInfo]] = run(StreamInfo.xInfoConsumers(key, group))

  /**
    * Deletes the given entries with an explicit deletion `policy`; one [[sage.commands.StreamEntryDeletion]] per ID (`XDELEX`).
    */
  final def xDelEx(key: K, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: StreamId,
    rest: StreamId*
  ): F[Vector[StreamEntryDeletion]] =
    run(Streams.xDelEx(key, policy)(first, rest*))

  /**
    * Acknowledges and deletes the given entries in one step (`XACKDEL`); one [[sage.commands.StreamEntryDeletion]] per ID.
    */
  final def xAckDel(key: K, group: String, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: StreamId,
    rest: StreamId*
  ): F[Vector[StreamEntryDeletion]] = run(Streams.xAckDel(key, group, policy)(first, rest*))

  /**
    * Negatively-acknowledges the given pending entries (`XNACK`), per `mode`; returns how many were affected. See [[sage.commands.NackMode]].
    */
  final def xNack(key: K, group: String, mode: NackMode)(first: StreamId, rest: StreamId*)(
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): F[Long] = run(Streams.xNack(key, group, mode)(first, rest*)(retryCount, force))

  // --- scripting -----------------------------------------------------------------------------------------------------------------------

  /**
    * Evaluates the Lua `script`, returning its reply as a raw [[sage.protocol.Frame]] for the caller to decode.
    */
  final def eval(script: String): F[Frame] = run(Scripting.eval(script))

  /**
    * Evaluates the Lua `script` with the given keys.
    */
  final def eval(script: String, keys: Seq[K]): F[Frame] = run(Scripting.eval(script, keys))

  /**
    * Evaluates the Lua `script` with the given keys and arguments.
    */
  final def eval[V: ValueCodec](script: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.eval(script, keys, args))

  /**
    * Read-only [[eval]] (`EVAL_RO`), eligible for replica routing; the script must not write.
    */
  final def evalRo(script: String): F[Frame] = run(Scripting.evalRo(script))

  /**
    * Read-only [[eval]] with the given keys (`EVAL_RO`).
    */
  final def evalRo(script: String, keys: Seq[K]): F[Frame] = run(Scripting.evalRo(script, keys))

  /**
    * Read-only [[eval]] with the given keys and arguments (`EVAL_RO`).
    */
  final def evalRo[V: ValueCodec](script: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalRo(script, keys, args))

  /**
    * Evaluates a previously [[scriptLoad]]ed script by its `sha` (`EVALSHA`); fails with a `NOSCRIPT` server error if uncached.
    */
  final def evalSha(sha: String): F[Frame] = run(Scripting.evalSha(sha))

  /**
    * Evaluates a cached script by `sha` with the given keys (`EVALSHA`).
    */
  final def evalSha(sha: String, keys: Seq[K]): F[Frame] = run(Scripting.evalSha(sha, keys))

  /**
    * Evaluates a cached script by `sha` with the given keys and arguments (`EVALSHA`).
    */
  final def evalSha[V: ValueCodec](sha: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalSha(sha, keys, args))

  /**
    * Read-only [[evalSha]] (`EVALSHA_RO`), eligible for replica routing.
    */
  final def evalShaRo(sha: String): F[Frame] = run(Scripting.evalShaRo(sha))

  /**
    * Read-only [[evalSha]] with the given keys (`EVALSHA_RO`).
    */
  final def evalShaRo(sha: String, keys: Seq[K]): F[Frame] = run(Scripting.evalShaRo(sha, keys))

  /**
    * Read-only [[evalSha]] with the given keys and arguments (`EVALSHA_RO`).
    */
  final def evalShaRo[V: ValueCodec](sha: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalShaRo(sha, keys, args))

  /**
    * Loads a script into the server's cache without running it, returning its SHA1 for [[evalSha]].
    */
  final def scriptLoad(script: String): F[String] = run(Scripting.scriptLoad(script))

  /**
    * Returns, for each given SHA, whether that script is in the cache.
    */
  final def scriptExists(first: String, rest: String*): F[Vector[Boolean]] = run(Scripting.scriptExists(first, rest*))

  /**
    * Flushes the script cache.
    */
  final def scriptFlush(mode: Option[FlushMode] = None): F[Unit] = run(Scripting.scriptFlush(mode))

  /**
    * Kills the currently-executing script, if it has not yet written.
    */
  final def scriptKill: F[Unit] = run(Scripting.scriptKill)

  /**
    * Returns the source of a cached script by `sha` (Valkey `SCRIPT SHOW`).
    */
  final def scriptShow(sha: String): F[String] = run(Scripting.scriptShow(sha))

  // --- functions -----------------------------------------------------------------------------------------------------------------------

  /**
    * Calls the named Function, returning its reply as a raw [[sage.protocol.Frame]] for the caller to decode (`FCALL`).
    */
  final def fCall(function: String): F[Frame] = run(Functions.fCall(function))

  /**
    * Calls the named Function with the given keys (`FCALL`).
    */
  final def fCall(function: String, keys: Seq[K]): F[Frame] = run(Functions.fCall(function, keys))

  /**
    * Calls the named Function with the given keys and arguments (`FCALL`).
    */
  final def fCall[V: ValueCodec](function: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Functions.fCall(function, keys, args))

  /**
    * Read-only [[fCall]] (`FCALL_RO`), eligible for replica routing; the Function must not write.
    */
  final def fCallRo(function: String): F[Frame] = run(Functions.fCallRo(function))

  /**
    * Read-only [[fCall]] with the given keys (`FCALL_RO`).
    */
  final def fCallRo(function: String, keys: Seq[K]): F[Frame] = run(Functions.fCallRo(function, keys))

  /**
    * Read-only [[fCall]] with the given keys and arguments (`FCALL_RO`).
    */
  final def fCallRo[V: ValueCodec](function: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Functions.fCallRo(function, keys, args))

  /**
    * Loads a Library of Functions from `code`, returning the Library name; `replace` overwrites an existing one.
    */
  final def functionLoad(code: String, replace: Boolean = false): F[String] = run(Functions.functionLoad(code, replace))

  /**
    * Deletes a whole Library by name.
    */
  final def functionDelete(libraryName: String): F[Unit] = run(Functions.functionDelete(libraryName))

  /**
    * Removes all Libraries.
    */
  final def functionFlush(mode: Option[FlushMode] = None): F[Unit] = run(Functions.functionFlush(mode))

  /**
    * Kills the currently-executing Function, if it has not yet written.
    */
  final def functionKill: F[Unit] = run(Functions.functionKill)

  /**
    * Returns a serialized snapshot of all Libraries for use with [[functionRestore]].
    */
  final def functionDump: F[Bytes] = run(Functions.functionDump)

  /**
    * Restores Libraries from a [[functionDump]] payload, per the given [[sage.commands.RestorePolicy]].
    */
  final def functionRestore(payload: Bytes, policy: Option[RestorePolicy] = None): F[Unit] =
    run(Functions.functionRestore(payload, policy))

  /**
    * Lists loaded Libraries and their Functions, optionally filtered to one Library; see [[sage.commands.LibraryInfo]].
    */
  final def functionList(libraryName: Option[String] = None, withCode: Boolean = false): F[Vector[LibraryInfo]] =
    run(Functions.functionList(libraryName, withCode))

  /**
    * Returns the running-script and per-engine statistics; see [[sage.commands.FunctionStats]].
    */
  final def functionStats: F[FunctionStats] = run(Functions.functionStats)

  // --- server admin --------------------------------------------------------------------------------------------------------------------

  /**
    * Returns the values of the given configuration parameters (glob patterns allowed).
    */
  final def configGet(parameter: String, rest: String*): F[Map[String, String]] = run(Server.configGet(parameter, rest*))

  /**
    * Sets the given configuration parameters at runtime.
    */
  final def configSet(setting: (String, String), rest: (String, String)*): F[Unit] = run(Server.configSet(setting, rest*))

  /**
    * Returns server `INFO`, optionally restricted to the given sections.
    */
  final def info(sections: String*): F[String] = run(Server.info(sections*))

  /**
    * Returns the number of keys in the current database.
    */
  final def dbSize: F[Long] = run(Server.dbSize)

  /**
    * Returns the server's current time.
    */
  final def time: F[Instant] = run(Server.time)

  /**
    * Returns this node's replication role; see [[sage.commands.Role]].
    */
  final def role: F[Role] = run(Server.role)

  /**
    * Removes all keys from all databases.
    */
  final def flushAll(mode: Option[FlushMode] = None): F[Unit] = run(Server.flushAll(mode))

  /**
    * Removes all keys from the current database.
    */
  final def flushDb(mode: Option[FlushMode] = None): F[Unit] = run(Server.flushDb(mode))

  /**
    * Blocks until `numReplicas` replicas have acknowledged prior writes or `timeout` elapses; returns how many did (`WAIT`).
    */
  final def waitReplicas(numReplicas: Long, timeout: FiniteDuration): F[Long] = run(Server.waitReplicas(numReplicas, timeout))

  /**
    * Blocks until prior writes are persisted to the local AOF and `numReplicas` replica AOFs, or `timeout` (`WAITAOF`).
    */
  final def waitAof(numLocal: Long, numReplicas: Long, timeout: FiniteDuration): F[(Long, Long)] =
    run(Server.waitAof(numLocal, numReplicas, timeout))

  /**
    * Returns the approximate memory used by the value at `key` in bytes, or `None` if absent.
    */
  final def memoryUsage(key: K, samples: Option[Long] = None): F[Option[Long]] = run(Server.memoryUsage(key, samples))

  /**
    * Asks the allocator to release memory back to the OS.
    */
  final def memoryPurge: F[Unit] = run(Server.memoryPurge)

  /**
    * Returns recent slow-log entries, newest first; see [[sage.commands.SlowLogEntry]].
    */
  final def slowLogGet(count: Option[Long] = None): F[Vector[SlowLogEntry]] = run(Server.slowLogGet(count))

  /**
    * Returns the number of entries in the slow log.
    */
  final def slowLogLen: F[Long] = run(Server.slowLogLen)

  /**
    * Clears the slow log.
    */
  final def slowLogReset: F[Unit] = run(Server.slowLogReset)

  /**
    * Returns recent command-log entries of the given type; see [[sage.commands.CommandLogEntry]]/[[sage.commands.CommandLogType]].
    */
  final def commandLogGet(count: Long, logType: CommandLogType): F[Vector[CommandLogEntry]] = run(Server.commandLogGet(count, logType))

  /**
    * Returns the number of entries in the command log of the given type.
    */
  final def commandLogLen(logType: CommandLogType): F[Long] = run(Server.commandLogLen(logType))

  /**
    * Clears the command log of the given type.
    */
  final def commandLogReset(logType: CommandLogType): F[Unit] = run(Server.commandLogReset(logType))

  /**
    * Returns recorded latency spikes for `event` as (timestamp, latency) pairs.
    */
  final def latencyHistory(event: String): F[Vector[(Instant, FiniteDuration)]] = run(Server.latencyHistory(event))

  /**
    * Returns the latest latency spike per monitored event; see [[sage.commands.LatencyEntry]].
    */
  final def latencyLatest: F[Vector[LatencyEntry]] = run(Server.latencyLatest)

  /**
    * Resets latency tracking for the given events (or all), returning how many were reset.
    */
  final def latencyReset(events: String*): F[Long] = run(Server.latencyReset(events*))

  /**
    * Returns per-command latency histograms; see [[sage.commands.CommandHistogram]].
    */
  final def latencyHistogram(commands: String*): F[Map[String, CommandHistogram]] = run(Server.latencyHistogram(commands*))

  /**
    * Returns the total number of commands the server knows.
    */
  final def commandCount: F[Long] = run(Server.commandCount)

  /**
    * Lists command names, optionally filtered; see [[sage.commands.CommandFilterBy]].
    */
  final def commandList(filterBy: Option[CommandFilterBy] = None): F[Vector[String]] = run(Server.commandList(filterBy))

  /**
    * Returns the keys a given command invocation would touch, as the server parses them.
    */
  final def commandGetKeys(command: String, args: String*): F[Vector[String]] = run(Server.commandGetKeys(command, args*))

  /**
    * Like [[commandGetKeys]], but pairs each key with its access flags.
    */
  final def commandGetKeysAndFlags(command: String, args: String*): F[Vector[(String, Set[String])]] =
    run(Server.commandGetKeysAndFlags(command, args*))

  /**
    * Returns metadata for the given commands (or all); see [[sage.commands.CommandInfo]].
    */
  final def commandInfo(commands: String*): F[Vector[CommandInfo]] = run(Server.commandInfo(commands*))

  /**
    * Returns `CLUSTER INFO` text about the cluster's state, as seen by this node.
    */
  final def clusterInfo: F[String] = run(Server.clusterInfo)

  /**
    * Returns `CLUSTER NODES` text describing every known node.
    */
  final def clusterNodes: F[String] = run(Server.clusterNodes)

  /**
    * Returns this node's cluster ID.
    */
  final def clusterMyId: F[String] = run(Server.clusterMyId)

  /**
    * Returns the hash slot a key name maps to.
    */
  final def clusterKeySlot(key: String): F[Long] = run(Server.clusterKeySlot(key))

  /**
    * Returns the number of keys this node holds in the given slot.
    */
  final def clusterCountKeysInSlot(slot: Int): F[Long] = run(Server.clusterCountKeysInSlot(slot))

  // --- access control ------------------------------------------------------------------------------------------------------------------

  /**
    * Returns the username of the current connection.
    */
  final def aclWhoAmI: F[String] = run(Acl.aclWhoAmI)

  /**
    * Returns the ACL rules, one line per user, in the `ACL SETUSER` format.
    */
  final def aclList: F[Vector[String]] = run(Acl.aclList)

  /**
    * Returns the list of configured usernames.
    */
  final def aclUsers: F[Vector[String]] = run(Acl.aclUsers)

  /**
    * Lists ACL command categories, or the commands within one when `category` is given.
    */
  final def aclCat(category: Option[String] = None): F[Vector[String]] = run(Acl.aclCat(category))

  /**
    * Returns the rules for one user, or `None` if there is no such user; see [[sage.commands.AclUser]].
    */
  final def aclGetUser(username: String): F[Option[AclUser]] = run(Acl.aclGetUser(username))

  /**
    * Returns recent ACL security events (failed auth/permission denials); see [[sage.commands.AclLogEntry]].
    */
  final def aclLog(count: Option[Long] = None): F[Vector[AclLogEntry]] = run(Acl.aclLog(count))

  // --- connection ----------------------------------------------------------------------------------------------------------------------

  /**
    * Returns `message` unchanged.
    */
  final def echo(message: String): F[String] = run(Connection.echo(message))

  /**
    * Returns the unique ID of the connection that served this command.
    */
  final def clientId: F[Long] = run(Connection.clientId)

  /**
    * Returns the connection's name (set via configuration), or empty if unset.
    */
  final def clientGetName: F[String] = run(Connection.clientGetName)

  /**
    * Returns a one-line description of the connection that served this command.
    */
  final def clientInfo: F[String] = run(Connection.clientInfo)

  /**
    * Returns one line per client connected to the server.
    */
  final def clientList: F[String] = run(Connection.clientList)

  /**
    * Returns the client ID this connection's tracking invalidations are redirected to, or 0 if none.
    */
  final def clientGetRedir: F[Long] = run(Connection.clientGetRedir)

  // --- arrays --------------------------------------------------------------------------------------------------------------------------

  /**
    * Writes the given values into the Array at `key` from `index` onward, returning the new used count.
    */
  final def arSet[V: ValueCodec](key: K, index: Long, first: V, rest: V*): F[Long] = run(Arrays.arSet(key, index, first, rest*))

  /**
    * Writes values at explicit indices in the Array at `key`, returning the new used count.
    */
  final def arMSet[V: ValueCodec](key: K, first: (Long, V), rest: (Long, V)*): F[Long] = run(Arrays.arMSet(key, first, rest*))

  /**
    * Returns the value at `index` in the Array at `key`, or `None` if that slot is empty.
    */
  final def arGet[V: ValueCodec](key: K, index: Long): F[Option[V]] = run(Arrays.arGet(key, index))

  /**
    * Returns the values at the given indices in the Array at `key`, each `None` if its slot is empty.
    */
  final def arMGet[V: ValueCodec](key: K, first: Long, rest: Long*): F[Vector[Option[V]]] = run(Arrays.arMGet(key, first, rest*))

  /**
    * Returns the logical length of the Array at `key` (highest index + 1).
    */
  final def arLen(key: K): F[Long] = run(Arrays.arLen(key))

  /**
    * Returns the number of populated slots in the Array at `key`.
    */
  final def arCount(key: K): F[Long] = run(Arrays.arCount(key))

  /**
    * Returns the values in the index range `[start, end]` of the Array at `key`, each `None` if its slot is empty.
    */
  final def arGetRange[V: ValueCodec](key: K, start: Long, end: Long): F[Vector[Option[V]]] =
    run(Arrays.arGetRange(key, start, end))

  /**
    * Appends values to the Array at `key` in ring-buffer mode of capacity `size`, overwriting the oldest; returns the new used count.
    */
  final def arRing[V: ValueCodec](key: K, size: Long, first: V, rest: V*): F[Long] = run(Arrays.arRing(key, size, first, rest*))

  /**
    * Returns the last `count` populated values of the Array at `key`, newest-first when `rev`.
    */
  final def arLastItems[V: ValueCodec](key: K, count: Long, rev: Boolean = false): F[Vector[V]] =
    run(Arrays.arLastItems(key, count, rev))

  /**
    * Clears the given indices of the Array at `key`, returning how many were populated.
    */
  final def arDel(key: K, first: Long, rest: Long*): F[Long] = run(Arrays.arDel(key, first, rest*))

  /**
    * Clears the given inclusive index ranges of the Array at `key`, returning how many slots were cleared.
    */
  final def arDelRange(key: K, first: (Long, Long), rest: (Long, Long)*): F[Long] = run(Arrays.arDelRange(key, first, rest*))

  /**
    * Writes the given values at the Array's write cursor and advances it, returning the new used count.
    */
  final def arInsert[V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Arrays.arInsert(key, first, rest*))

  /**
    * Returns the next populated index at or after the write cursor, or `None` if none.
    */
  final def arNext(key: K): F[Option[Long]] = run(Arrays.arNext(key))

  /**
    * Moves the Array's write cursor to `index`, returning whether it moved.
    */
  final def arSeek(key: K, index: Long): F[Boolean] = run(Arrays.arSeek(key, index))

  /**
    * Returns populated (index, value) pairs in `[start, end]`, up to `limit`.
    */
  final def arScan[V: ValueCodec](key: K, start: Long, end: Long, limit: Option[Long] = None): F[Vector[(Long, V)]] =
    run(Arrays.arScan(key, start, end, limit))

  /**
    * Returns the indices in `[start, end]` whose values match the given criteria, combined per `combine`; see [[sage.commands.ArMatch]].
    */
  final def arGrep(
    key: K,
    start: Long,
    end: Long,
    combine: ArGrepCombine = ArGrepCombine.Or,
    limit: Option[Long] = None,
    noCase: Boolean = false
  )(first: ArMatch, rest: ArMatch*): F[Vector[Long]] = run(Arrays.arGrep(key, start, end, combine, limit, noCase)(first, rest*))

  /**
    * Like [[arGrep]], but returns each matching index paired with its value.
    */
  final def arGrepWithValues[V: ValueCodec](
    key: K,
    start: Long,
    end: Long,
    combine: ArGrepCombine = ArGrepCombine.Or,
    limit: Option[Long] = None,
    noCase: Boolean = false
  )(first: ArMatch, rest: ArMatch*): F[Vector[(Long, V)]] = run(Arrays.arGrepWithValues(key, start, end, combine, limit, noCase)(first, rest*))

  /**
    * Sums the values in `[start, end]` of the Array at `key`, or `None` if the range is empty.
    */
  final def arOpSum(key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpSum(key, start, end))

  /**
    * Returns the minimum value in `[start, end]`, or `None` if the range is empty.
    */
  final def arOpMin(key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpMin(key, start, end))

  /**
    * Returns the maximum value in `[start, end]`, or `None` if the range is empty.
    */
  final def arOpMax(key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpMax(key, start, end))

  /**
    * Returns the bitwise AND of the integer values in `[start, end]`, or `None` if the range is empty.
    */
  final def arOpAnd(key: K, start: Long, end: Long): F[Option[Long]] = run(Arrays.arOpAnd(key, start, end))

  /**
    * Returns the bitwise OR of the integer values in `[start, end]`, or `None` if the range is empty.
    */
  final def arOpOr(key: K, start: Long, end: Long): F[Option[Long]] = run(Arrays.arOpOr(key, start, end))

  /**
    * Returns the bitwise XOR of the integer values in `[start, end]`, or `None` if the range is empty.
    */
  final def arOpXor(key: K, start: Long, end: Long): F[Option[Long]] = run(Arrays.arOpXor(key, start, end))

  /**
    * Returns how many slots in `[start, end]` are populated.
    */
  final def arOpUsed(key: K, start: Long, end: Long): F[Long] = run(Arrays.arOpUsed(key, start, end))

  /**
    * Returns how many slots in `[start, end]` hold `value`.
    */
  final def arOpMatch[V: ValueCodec](key: K, start: Long, end: Long, value: V): F[Long] =
    run(Arrays.arOpMatch(key, start, end, value))

  /**
    * Returns summary metadata about the Array at `key`; see [[sage.commands.ArrayInfo]].
    */
  final def arInfo(key: K): F[ArrayInfo] = run(Arrays.arInfo(key))

  /**
    * Returns the full introspection of the Array at `key`; see [[sage.commands.ArrayInfoFull]].
    */
  final def arInfoFull(key: K): F[ArrayInfoFull] = run(Arrays.arInfoFull(key))
}

/**
  * The user-facing handle owning all connections to one server: the command surface, plus pipelines and transactions. A batch of
  * [[sage.commands.Commands]] handed to `pipeline` is sent in one round-trip, yielding a typed result per command.
  */
// one independent keyspace a full SCAN sweep visits: a single node standalone, one per slot-owning master in cluster. `node` is None when
// the backend has no node to pin to (standalone, or a not-yet-discovered cluster topology), in which case the command runs through normal routing.
final private[sage] case class ScanTarget(node: Option[Node])

private[sage] object ScanTarget {
  val any: ScanTarget = ScanTarget(None)
}

// drives the cluster-wide SCAN walk: each target is scanned to its node-local zero cursor before the next, so the sweep never silently
// covers a single node.
private[sage] enum ScanStep {
  case Begin
  case Visit(cursor: ScanCursor, remaining: Vector[ScanTarget])
  case End
}

/**
  * The user-facing client over an effect `F`, aliased per backend as `sage.backend.SageClient` (e.g. `Client[Task]` in the ZIO build). It owns all
  * connections to one server or cluster and exposes the whole command catalogue through the inherited [[CommandRunner]] sugar (`get`, `set`,
  * …), each delegating to [[CommandRunner.run]]. Ordinary commands are auto-pipelined onto the Multiplexed Connection; only [[transaction]],
  * blocking commands, and pub/sub acquire other connections, transparently. Constructed with a backend's `connect`/`scoped` and released with
  * [[close]].
  */
trait Client[F[_], K] extends CommandRunner[F, K] {

  /**
    * Runs a read with client-side caching: served from the local cache until a server invalidation push or `ttl` evicts it. Only a
    * cacheable command with at least one key qualifies — a read whose result is a pure function of its keys' state, so an invalidation
    * covers every change. A write, a keyless read, or a time-varying/non-deterministic read (`TTL`, `SRANDMEMBER`) fails with
    * [[sage.SageException.NotCacheable]]. On a cluster client this currently runs the read without caching, so the same call stays
    * topology-portable.
    */
  def cached[A](command: Command[A], ttl: FiniteDuration): F[A]

  private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): F[Out]

  private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): F[R]

  /**
    * Runs a fixed-arity batch of commands in one round-trip, yielding a result tuple that mirrors the argument tuple element-for-element
    * (`pipeline((get, incr))` → `F[(Option[V], Long)]`). Fails if any position fails; use [[pipelineAttempt]] to keep per-position errors.
    */
  def pipeline[T <: NonEmptyTuple](commands: T)(using Tuple.IsMappedBy[Command][T]): F[Tuple.InverseMap[T, Command]] =
    pipeline(Pipeline.fromTuple(commands))

  /**
    * Runs a dynamic, homogeneous batch of commands in one round-trip, yielding one result per command in order. An empty batch is a no-op
    * that yields an empty `Vector` without touching the socket.
    */
  def pipeline[A](commands: Seq[Command[A]]): F[Vector[A]] =
    pipeline(Pipeline.sequence(commands))

  /**
    * Like the tuple [[pipeline]], but yields the per-position results, each slot a `Right`/`Left`, instead of failing on the first error.
    */
  def pipelineAttempt[T <: NonEmptyTuple](commands: T)(using Tuple.IsMappedBy[Command][T]): F[Tuple.Map[Tuple.InverseMap[T, Command], Attempt]] =
    pipelineAttempt(Pipeline.fromTuple(commands))

  /**
    * Like the `Seq` [[pipeline]], but yields the per-position results, each slot a `Right`/`Left`, instead of failing on the first error.
    */
  def pipelineAttempt[A](commands: Seq[Command[A]]): F[Vector[Attempt[A]]] =
    pipelineAttempt(Pipeline.sequence(commands))

  /**
    * Opens a [[TransactionScope]] on a leased Dedicated Connection for `MULTI`/`EXEC`, optionally guarded by `WATCH`.
    */
  def transaction[A](body: TransactionScope[F, K] => F[A]): F[A]

  /**
    * Subscribes to classic channels, returning a [[Subscription]] handle the backend wraps into its native stream of [[Message]]s.
    */
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]]

  /**
    * Subscribes to glob patterns, yielding [[PatternMessage]]s that also name the matched pattern.
    */
  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): F[Subscription[F, PatternMessage[V]]]

  /**
    * Subscribes to Shard Channels; in a cluster each is routed to the Node owning its Slot, and a delivery surfaces as an ordinary [[Message]].
    */
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]]

  // the keyspaces a full SCAN sweep must visit (one per slot-owning master in cluster); runOn pins the keyless SCAN page to a node so a
  // cursor resumes where it came from
  private[sage] def scanTargets: F[Vector[ScanTarget]]

  private[sage] def runOn[A](target: ScanTarget, command: Command[A]): F[A]

  /**
    * Releases all connections and the client's resources.
    */
  def close: F[Unit]

  /**
    * Re-types the whole client surface to another key type over the same connection — no new connection is opened. Everything but the command
    * sugar is key-independent and simply forwards (`cached`, `pipeline`, `subscribe*`, `runOn`, `close`); `transaction` re-types its scope. This
    * is what makes `client.as[Array[Byte]]` keep the full surface (and the backend `…All`/`subscribe` helpers, which extend `Client[F, K]`),
    * not just the command methods.
    */
  override def as[K2](using KeyCodec[K2]): Client[F, K2] = {
    val self = this
    new Client[F, K2] {
      def run[A](command: Command[A]): F[A]                                     = self.run(command)
      def cached[A](command: Command[A], ttl: FiniteDuration): F[A]             = self.cached(command, ttl)
      private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): F[Out]           = self.pipeline(p)
      private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): F[R]      = self.pipelineAttempt(p)
      def transaction[A](body: TransactionScope[F, K2] => F[A]): F[A]           = self.transaction(scope => body(scope.as[K2]))
      def subscribeChannels[V: ValueCodec](channel: String, rest: String*)      = self.subscribeChannels(channel, rest*)
      def subscribePatterns[V: ValueCodec](pattern: String, rest: String*)      = self.subscribePatterns(pattern, rest*)
      def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*) = self.subscribeShardChannels(channel, rest*)
      private[sage] def scanTargets: F[Vector[ScanTarget]]                      = self.scanTargets
      private[sage] def runOn[A](target: ScanTarget, command: Command[A]): F[A] = self.runOn(target, command)
      def close: F[Unit]                                                        = self.close
    }
  }
}

object Client {

  private val defaults = SageConfig()

  // a cacheable read's result is a pure function of its keys' state (Command.cacheable) and it names at least one key — a keyless read
  // could only ever be evicted by TTL, never by an invalidation push, so it is rejected rather than allowed to silently go stale
  private[internal] def cacheable(command: Command[?]): Boolean = command.cacheable && command.keyIndices.nonEmpty

  private[internal] def notCacheable(command: Command[?]): NotCacheable =
    NotCacheable(s"${command.name} is not cacheable: cached requires a cacheable command with at least one key")

  // if the submit throws synchronously (e.g. a closed transport) instead of registering a callback, complete with the error so CIO.async settles
  private[internal] def completing[A](complete: Try[A] => Unit)(submit: => Unit): Unit =
    try submit
    catch { case NonFatal(error) => complete(Failure(error)) }

  // attributes the node at the completion site, so a batch that never reaches the wire (a false submit) leaves its callbacks unattributed
  private def attributeOnComplete(cb: Try[Any] => Unit, node: Node): Try[Any] => Unit =
    result => { Events.attributeNode(cb, node); cb(result) }

  // a pipeline batched onto a single connection: scatter each reply into its submission-order slot, and on a disconnect report a failed
  // completion per position before failing the effect once. Shared by standalone/master-replica; the cluster runtime splits per node instead.
  private[internal] def submitBatchOnOne(
    events: Events,
    commands: Vector[Command[?]],
    spans: Vector[CommandSpan],
    submitAll: (Vector[Command[?]], Vector[Try[Any] => Unit]) => Boolean,
    complete: Try[Vector[Either[SageException, Any]]] => Unit,
    node: Option[Node] = None
  ): Unit = {
    val collector = new TxSupport.IndexedCollector[Either[SageException, Any]](commands.length, complete)
    val tracked   = Vector.tabulate(commands.length) { i =>
      val span = if (spans.isEmpty) CommandSpan.noop else spans(i)
      Events.trackCommand[Any](events, commands(i), (result: Try[Any]) => collector.set(i, TxSupport.toEither(result)), span)
    }
    val callbacks = node match {
      case Some(n) => tracked.map(attributeOnComplete(_, n))
      case None    => tracked
    }
    if (!submitAll(commands, callbacks)) {
      val error = NotConnected()
      tracked.foreach(Events.abandonSpan(_, error))
      if (events.emitsEvents)
        commands.foreach(c => events.emit(SageEvent.CommandCompleted(c.name, None, Duration.Zero, Outcome.Failed(error))))
      complete(Failure(error))
    }
  }

  /**
    * The construction entry point each backend's `connect`/`scoped` builds on: validates `config`, then connects per its [[Topology]].
    */
  def connect(config: SageConfig): CIO[Client[CIO, String]] =
    validate(config) match {
      case Some(problem) => CIO.fail(new IllegalArgumentException(problem))
      case None          =>
        config.topology match {
          case Topology.Standalone(endpoint)                => connectStandalone(config, endpoint)
          case Topology.Cluster(seeds, clusterConfig)       =>
            ClusterLive.connect(config, seeds.map(e => Node(e.host, e.port)), clusterConfig, Scheduler.real, translateHandshake)
          case Topology.MasterReplica(seeds, masterReplica) =>
            MasterReplicaLive.connect(config, seeds.map(e => Node(e.host, e.port)), masterReplica, Scheduler.real, translateHandshake)
        }
    }

  // a misconfigured client is a programmer error, surfaced through the connect effect (never thrown from a constructor) and deliberately
  // outside the sealed hierarchy, like the other usage guards
  private def validate(config: SageConfig): Option[String] = {
    // pingInterval/pingTimeout are inert when the watchdog is disabled, so don't reject them then
    val watchdog =
      if (config.watchdog.enabled)
        Vector(
          atLeastOneMilli(config.watchdog.pingInterval, "watchdog.pingInterval"),
          atLeastOneMilli(config.watchdog.pingTimeout, "watchdog.pingTimeout")
        )
      else Vector.empty
    val checks   = Vector(
      cond(config.database >= 0, "database must be >= 0"),
      atLeastOneMilli(config.connectTimeout, "connectTimeout"),
      atLeastOneMilli(config.closeTimeout, "closeTimeout"),
      atLeastOneMilli(config.reconnect.initialDelay, "reconnect.initialDelay"),
      cond(config.reconnect.maxDelay >= config.reconnect.initialDelay, "reconnect.maxDelay must be >= initialDelay"),
      cond(config.reconnect.multiplier >= 1.0, "reconnect.multiplier must be >= 1.0"),
      cond(config.dedicatedPool.maxConnections >= 1, "dedicatedPool.maxConnections must be >= 1"),
      atLeastOneMilli(config.dedicatedPool.acquireTimeout, "dedicatedPool.acquireTimeout"),
      atLeastOneMilliOrInfinite(config.dedicatedPool.idleTimeout, "dedicatedPool.idleTimeout"),
      cond(!config.clientCache.enabled || config.clientCache.maxBytes > 0L, "clientCache.maxBytes must be positive when caching is enabled"),
      cond(config.pubsub.bufferSize >= 1, "pubsub.bufferSize must be >= 1")
    ) ++ watchdog ++ (config.topology match {
      case Topology.Cluster(seeds, cluster)             =>
        Vector(
          cond(seeds.nonEmpty, "cluster topology requires at least one seed"),
          cond(cluster.maxRedirects >= 0, "cluster.maxRedirects must be >= 0"),
          atLeastOneMilli(cluster.minRefreshInterval, "cluster.minRefreshInterval")
        ) ++ seeds.map(s => port(s.port, s"seed ${s.host}:${s.port} port"))
      case Topology.MasterReplica(seeds, masterReplica) =>
        Vector(
          cond(seeds.nonEmpty, "master-replica topology requires at least one seed"),
          atLeastOneMilli(masterReplica.minRefreshInterval, "masterReplica.minRefreshInterval")
        ) ++ seeds.map(s => port(s.port, s"seed ${s.host}:${s.port} port"))
      // a Standalone has no replicas, so the strict Replica policy could never serve a read; the *Preferred policies harmlessly degrade to
      // the one node, so they stay valid (a readFrom can then be shared across environments)
      case Topology.Standalone(endpoint)                =>
        Vector(
          port(endpoint.port, s"endpoint ${endpoint.host}:${endpoint.port} port"),
          cond(config.readFrom != ReadFrom.Replica, "readFrom = Replica needs replicas; a Standalone topology has none")
        )
    })
    checks.flatten.headOption
  }

  private def cond(ok: Boolean, problem: String): Option[String]                        = if (ok) None else Some(problem)
  private def port(value: Int, label: String): Option[String]                           = cond(value >= 1 && value <= 65535, s"$label must be in 1..65535")
  private def atLeastOneMilli(value: FiniteDuration, label: String): Option[String]     = cond(value.toMillis >= 1L, s"$label must be at least 1ms")
  private def atLeastOneMilliOrInfinite(value: Duration, label: String): Option[String] =
    cond(value == Duration.Inf || (value.isFinite && value.toMillis >= 1L), s"$label must be at least 1ms (or Inf)")

  private def connectStandalone(config: SageConfig, endpoint: Endpoint): CIO[Client[CIO, String]] =
    // build the TLS context once (eager failure on bad trust material), then capture it in the reconnect factory so every connection — the
    // multiplexed one and each dedicated one — is upgraded identically
    CIO.blocking(Tls.buildUpgrade(config.tls, endpoint.host, endpoint.port)).flatMap { upgrade =>
      connectWith(
        (onFrame, onClosed) => SocketTransport.connect(endpoint.host, endpoint.port, config.connectTimeout, upgrade, onFrame, onClosed),
        Scheduler.real,
        config.reconnect,
        config.watchdog,
        config.connectTimeout,
        config.closeTimeout,
        config.dedicatedPool,
        config.pubsub,
        config.auth,
        config.clientCache.maxBytes,
        config.clientCache.enabled,
        Events(config.listeners, config.tracer, serverNode = Some(Node(endpoint.host, endpoint.port))),
        config.database,
        config.clientName
      )
    }

  // The HELLO 3 handshake is the bootstrap re-run on every (re)connection; the first connect propagates its failure, reconnects retry it.
  private[client] def connectWith(
    factory: MultiplexedConnection.TransportFactory,
    scheduler: Scheduler = Scheduler.real,
    reconnect: BackoffConfig = defaults.reconnect,
    watchdog: WatchdogConfig = defaults.watchdog,
    connectTimeout: FiniteDuration = defaults.connectTimeout,
    closeTimeout: FiniteDuration = defaults.closeTimeout,
    dedicatedPool: DedicatedPoolConfig = defaults.dedicatedPool,
    pubsub: PubSubConfig = defaults.pubsub,
    auth: Option[AuthConfig] = None,
    cacheMaxBytes: Long = defaults.clientCache.maxBytes,
    cachingEnabled: Boolean = defaults.clientCache.enabled,
    events: Events = Events.disabled,
    database: Int = 0,
    clientName: Option[String] = None
  ): CIO[Client[CIO, String]] = {
    val bootstrap            = Bootstrap.commands(auth, database, clientName)
    // only the Multiplexed Connection caches reads, so only it enables tracking; the dedicated pool and subscription connection keep the
    // plain bootstrap. Tracking is skipped entirely when caching is disabled, so a server that denies CLIENT TRACKING still connects.
    val multiplexedBootstrap = if (cachingEnabled) bootstrap :+ Connection.clientTrackingOnOptin else bootstrap
    CIO
      .blocking(
        MultiplexedConnection
          .connect(factory, scheduler, multiplexedBootstrap, reconnect, watchdog, connectTimeout, closeTimeout, cacheMaxBytes, None, events)
      )
      .map { connection =>
        val pool          = DedicatedPool.forConnection(factory, bootstrap, scheduler, connection, dedicatedPool, connectTimeout.toMillis)
        // lazy: no socket is opened until the first subscription, and it is gated on the Multiplexed Connection being live
        val subscriptions = new SubscriptionConnection(
          factory,
          bootstrap,
          scheduler,
          reconnect,
          watchdog,
          connectTimeout.toMillis,
          pubsub.bufferSize,
          () => connection.isLive,
          events = events
        )
        new Live(connection, pool, subscriptions, cachingEnabled, events)
      }
      .mapError { error => events.close(); translateHandshake(error) }
  }

  // pre-6.0 Redis answers HELLO with an unknown-command error; newer servers reject an unsupported protocol version with NOPROTO. A
  // rejected certificate or hostname mismatch surfaces from the handshake as an SSLException, which would otherwise escape the sealed
  // hierarchy.
  private def translateHandshake(error: Throwable): Throwable =
    error match {
      case e: ServerError if e.code == "NOPROTO" || e.getMessage.toLowerCase.contains("unknown command") =>
        UnsupportedServer(s"sage requires RESP3 (Redis 6.0+ or any Valkey); server rejected HELLO 3: ${e.getMessage}")
      case e: SSLException                                                                               =>
        TlsError(s"TLS handshake failed: ${e.getMessage}")
      case e: SageException                                                                              => e
      // a raw network error would otherwise escape the sealed hierarchy
      case other                                                                                         =>
        val failed = ConnectionFailed(s"could not connect: $other")
        failed.initCause(other)
        failed
    }

  final private class Live(
    connection: MultiplexedConnection,
    pool: DedicatedPool,
    subscriptions: SubscriptionConnection,
    cachingEnabled: Boolean,
    events: Events
  ) extends Client[CIO, String] {

    def run[A](command: Command[A]): CIO[A] =
      command.execution match {
        case Execution.Ordinary =>
          CIO.async { callback =>
            val tracked = Events.trackCommand(events, command, callback)
            Client.completing(tracked)(connection.submit(command, tracked))
          }
        case Execution.Blocking =>
          // acquire a fresh lease per execution: a lease is single-shot (cancel is terminal), so a captured one would make a re-run of this value hang
          CIO.acquireReleaseWith(CIO.defer(new DedicatedPool.Lease))(lease => CIO.blocking(lease.cancel())) { lease =>
            CIO.async { callback =>
              val tracked = Events.trackCommand(events, command, callback)
              Client.completing(tracked)(pool.use(command, tracked, lease))
            }
          }
      }

    def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
      if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
      else if (!cachingEnabled) run(command) // tracking was never enabled, so run uncached rather than issue an unbacked CLIENT CACHING YES
      else CIO.async(callback => Client.completing(callback)(connection.cachedSubmit(command, ttl.toMillis, callback)))

    def scanTargets: CIO[Vector[ScanTarget]] = CIO.value(Vector(ScanTarget.any))

    def runOn[A](target: ScanTarget, command: Command[A]): CIO[A] = run(command)

    private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out] =
      submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))

    private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] =
      submitPipeline(p).map(p.toResults)

    // The lease is bracketed: release runs on success, failure, and interruption (IO.bracket). A clean exit (exec/discard cleared the
    // connection's WATCH/MULTI state, no reply outstanding) recycles the connection; a scope left armed or interrupted mid-command is
    // discarded rather than handed to the next borrower dirty.
    def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
      CIO.acquireReleaseWith(acquireScope)(releaseScope)(scope => CIO.unit.flatMap(_ => body(scope)))

    private def acquireScope: CIO[TxScope] =
      CIO.blocking {
        try new TxScope(pool.acquireForTransaction(), events = events)
        catch {
          case e: SageException => throw e
          case NonFatal(_)      => throw ConnectionLost(mayHaveExecuted = false)
        }
      }

    private def releaseScope(scope: TxScope): CIO[Unit] =
      CIO.blocking(pool.releaseTransaction(scope.conn, scope.sealAndReusable()))

    private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
      if (p.commands.isEmpty)
        CIO.value(Vector.empty)
      else if (p.commands.exists(_.isBlocking))
        CIO.fail(new IllegalArgumentException("a Pipeline cannot carry blocking commands; run them individually on the client"))
      else
        CIO.async { complete =>
          Client.submitBatchOnOne(events, p.commands, Events.startSpans(events, p.commands), connection.submitAll, complete)
        }

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
      CIO.blocking(channelMessages(subscriptions.subscribeChannels(channel +: rest.toVector)))

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
      CIO.blocking(patternMessages(subscriptions.subscribePatterns(pattern +: rest.toVector)))

    // a standalone server has no slots, so every Shard Channel rides the one Subscription Connection in shard mode
    def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
      CIO.blocking(channelMessages(subscriptions.subscribeShard(channel +: rest.toVector)))

    def close: CIO[Unit] = CIO.blocking { subscriptions.close(); pool.close(); connection.close(); events.close() }
  }

  // wrap a raw subscription as the effect-typed interface each backend lowers into its native stream; `build` returns None to end the stream
  private def messages[M](raw: SubscriptionConnection.RawSubscription)(
    build: SubscriptionConnection.Delivery => Option[M]
  ): Subscription[CIO, M] =
    new Subscription[CIO, M] {
      // async, not blocking: the reader thread completes the callback, so a fiber parks instead of pinning a runtime worker
      def next: CIO[Option[M]] = {
        // deregister the parked waiter if this next is interrupted, else a stale waiter trips the single-consumer guard
        val registered = new AtomicReference[Option[SubscriptionConnection.Delivery] => Unit]()
        CIO.ensure(CIO.defer { val cb = registered.getAndSet(null); if (cb ne null) raw.cancelNext(cb) }) {
          CIO.async { complete =>
            val cb: Option[SubscriptionConnection.Delivery] => Unit = {
              case Some(delivery) => complete(Try(build(delivery)))
              case None           => complete(Success(None))
            }
            registered.set(cb)
            raw.next(cb)
          }
        }
      }
      def close: CIO[Unit]     = CIO.blocking(raw.close())
    }

  // a channel/shard delivery is a Message
  private[internal] def channelMessages[V](raw: SubscriptionConnection.RawSubscription)(using ValueCodec[V]): Subscription[CIO, Message[V]] =
    messages(raw) {
      case SubscriptionConnection.Delivery.Channel(ch, payload) => Some(Message(ch, decodeOrThrow[V](payload)))
      case _                                                    => None
    }

  private[internal] def patternMessages[V](raw: SubscriptionConnection.RawSubscription)(using ValueCodec[V]): Subscription[CIO, PatternMessage[V]] =
    messages(raw) {
      case SubscriptionConnection.Delivery.Pattern(pat, ch, payload) => Some(PatternMessage(pat, ch, decodeOrThrow[V](payload)))
      case _                                                         => None
    }

  // fail the stream on a bad payload rather than dropping it
  private def decodeOrThrow[V](payload: sage.Bytes)(using codec: ValueCodec[V]): V =
    try
      codec.decode(payload) match {
        case Right(value) => value
        case Left(error)  => throw error
      }
    catch {
      case e: SageException => throw e
      case NonFatal(e)      => throw DecodeError.fromThrowable(e)
    }

  final private[internal] class TxScope(val conn: DedicatedConnection, onFault: Throwable => Unit = _ => (), events: Events = Events.disabled)
    extends TransactionScope[CIO, String] {

    // tracks whether watched keys may still be armed on the connection; set as soon as WATCH is attempted, cleared by EXEC/UNWATCH
    val armed = new AtomicBoolean(false)

    private def faulting[A](complete: Try[A] => Unit): Try[A] => Unit = {
      case failure @ Failure(error) => onFault(error); complete(failure)
      case success                  => complete(success)
    }

    // an EXEC fault arrives as an error *frame* in a Success, invisible to `faulting`, so scan the top level and the EXEC array
    private def refreshOnExecFault(frames: Vector[Frame]): Unit = {
      val nested = frames.lastOption match { case Some(Frame.Array(elems)) => elems.iterator; case _ => Iterator.empty[Frame] }
      (frames.iterator ++ nested).flatMap(TxSupport.errorOf).foreach(message => onFault(ServerError.of(message)))
    }

    // The lock makes "reject if released, else submit" atomic with the finalizer's seal-and-decide ([[sealAndReusable]]): a command
    // submitted under it is in-flight before the finalizer reads quiescence, so a handle captured past the block and raced against release
    // either submits onto a connection the finalizer then declines to recycle, or is rejected outright — never onto a re-borrowed one.
    private val lock     = new ReentrantLock()
    private var released = false

    private def submitting[A](complete: Try[A] => Unit)(submit: => Unit): Unit = {
      lock.lock()
      try
        if (released) complete(Failure(TxSupport.scopeReleasedError))
        else Client.completing(complete)(submit)
      finally lock.unlock()
    }

    // run once by the lease finalizer: seals the scope against further operations and reports whether the connection may be recycled
    private[internal] def sealAndReusable(): Boolean = {
      lock.lock()
      try {
        released = true
        conn.isHealthy && conn.isQuiescent && !armed.get
      } finally lock.unlock()
    }

    private def isReleased: Boolean = {
      lock.lock()
      try released
      finally lock.unlock()
    }

    def watch[K: KeyCodec](key: K, rest: K*): CIO[Unit] =
      CIO.async[Unit] { complete =>
        val watchCmd = Connection.watch(key, rest*)
        val tracked  = Events.trackSpan(events, watchCmd, complete)
        submitting(tracked) {
          armed.set(true)
          conn.submit(watchCmd, faulting(tracked))
        }
      }

    def run[A](command: Command[A]): CIO[A] =
      if (isReleased)
        CIO.fail(TxSupport.scopeReleasedError)
      else if (command.isBlocking)
        CIO.fail(new IllegalArgumentException("a Transaction cannot run blocking commands; run them individually on the client"))
      else
        CIO.async[A] { complete =>
          val tracked = Events.trackSpan(events, command, complete)
          submitting(tracked)(conn.submit(command, faulting(tracked)))
        }

    def discard: CIO[Unit] =
      CIO.async[Unit] { complete =>
        submitting(complete) {
          armed.set(false)
          conn.submit(Connection.unwatch, faulting(complete))
        }
      }

    private[sage] def exec[Out, R](p: Pipeline[Out, R]): CIO[Option[Out]] =
      runExec(p).flatMap {
        case None          => CIO.value(None)
        case Some(results) => TxSupport.collapseStrict(results, p.toOut).map(Some(_))
      }

    private[sage] def execAttempt[Out, R](p: Pipeline[Out, R]): CIO[Option[R]] =
      runExec(p).map(_.map(p.toResults))

    // None = WATCH abort; Some = the per-position decoded results. A queueing-phase error fails the effect (nothing ran).
    private def runExec[Out, R](p: Pipeline[Out, R]): CIO[Option[Vector[Either[SageException, Any]]]] =
      if (isReleased)
        CIO.fail(TxSupport.scopeReleasedError)
      // a truly empty no-op only when nothing is watched; with watches armed we must still MULTI/EXEC so a concurrent change can abort it
      else if (p.commands.isEmpty && !armed.get)
        CIO.value(Some(Vector.empty))
      else if (p.commands.exists(_.isBlocking))
        CIO.fail(new IllegalArgumentException("a Transaction cannot carry blocking commands; run them individually on the client"))
      else
        CIO
          .async[Vector[Frame]] { complete =>
            val tracked = Events.trackSpan(events, Connection.multi, complete)
            submitting(tracked)(conn.submitRaw(Connection.multi +: p.commands :+ Connection.exec, faulting(tracked)))
          }
          .flatMap { frames =>
            armed.set(false) // EXEC clears WATCH/MULTI state server-side whether it committed or aborted
            refreshOnExecFault(frames)
            TxSupport.interpretExec(p.commands, frames)
          }
  }
}
