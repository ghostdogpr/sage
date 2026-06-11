package sage.client.internal

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import javax.net.ssl.SSLException

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Bytes, Message, PatternMessage, SageException}
import sage.SageException.{ConnectionLost, NotCacheable, NotConnected, ServerError, TimedOut, TlsError, UnsupportedServer}
import sage.client.{AuthConfig, BackoffConfig, DedicatedPoolConfig, PubSubConfig, SageConfig, Topology, WatchdogConfig}
import sage.cluster.Node
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*
import sage.protocol.Frame

/**
  * The command surface shared by [[Client]] and [[TransactionScope]]: every command as a concrete method delegating to [[run]], so anything
  * implementing `run` — a fake, or a backend adapter lowering `F` to its native effect — gets the whole catalogue.
  */
trait CommandRunner[F[_]] {

  def run[A](command: Command[A]): F[A]

  final def ping(message: Option[String] = None): F[String] = run(Connection.ping(message))

  final def append[K: KeyCodec, V: ValueCodec](key: K, value: V): F[Long] = run(Strings.append(key, value))

  final def decr[K: KeyCodec](key: K): F[Long] = run(Strings.decr(key))

  final def decrBy[K: KeyCodec](key: K, decrement: Long): F[Long] = run(Strings.decrBy(key, decrement))

  final def get[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Strings.get(key))

  final def getDel[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Strings.getDel(key))

  final def getEx[K: KeyCodec, V: ValueCodec](key: K, expiry: GetExpiry = GetExpiry.Keep): F[Option[V]] =
    run(Strings.getEx(key, expiry))

  final def getRange[K: KeyCodec, V: ValueCodec](key: K, start: Long, end: Long): F[V] = run(Strings.getRange(key, start, end))

  final def incr[K: KeyCodec](key: K): F[Long] = run(Strings.incr(key))

  final def incrBy[K: KeyCodec](key: K, increment: Long): F[Long] = run(Strings.incrBy(key, increment))

  final def incrByFloat[K: KeyCodec](key: K, increment: Double): F[Double] = run(Strings.incrByFloat(key, increment))

  final def mGet[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Vector[Option[V]]] = run(Strings.mGet(first, rest*))

  final def mSet[K: KeyCodec, V: ValueCodec](first: (K, V), rest: (K, V)*): F[Unit] = run(Strings.mSet(first, rest*))

  final def mSetNx[K: KeyCodec, V: ValueCodec](first: (K, V), rest: (K, V)*): F[Boolean] = run(Strings.mSetNx(first, rest*))

  final def set[K: KeyCodec, V: ValueCodec](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  ): F[Boolean] = run(Strings.set(key, value, expiry, condition))

  final def setGet[K: KeyCodec, V: ValueCodec](
    key: K,
    value: V,
    expiry: SetExpiry = SetExpiry.Clear,
    condition: SetCondition = SetCondition.Always
  ): F[Option[V]] = run(Strings.setGet(key, value, expiry, condition))

  final def setRange[K: KeyCodec, V: ValueCodec](key: K, offset: Long, value: V): F[Long] = run(Strings.setRange(key, offset, value))

  final def strLen[K: KeyCodec](key: K): F[Long] = run(Strings.strLen(key))

  final def lcs[K: KeyCodec, V: ValueCodec](key1: K, key2: K): F[V] = run(Strings.lcs(key1, key2))

  final def lcsLen[K: KeyCodec](key1: K, key2: K): F[Long] = run(Strings.lcsLen(key1, key2))

  final def lcsIdx[K: KeyCodec](key1: K, key2: K, minMatchLen: Option[Long] = None, withMatchLen: Boolean = false): F[LcsMatches] =
    run(Strings.lcsIdx(key1, key2, minMatchLen, withMatchLen))

  final def digest[K: KeyCodec](key: K): F[Option[String]] = run(Strings.digest(key))

  final def delex[K: KeyCodec, V: ValueCodec](key: K, condition: DelexCondition[V] = DelexCondition.Always): F[Boolean] =
    run(Strings.delex(key, condition))

  final def msetEx[K: KeyCodec, V: ValueCodec](
    condition: SetCondition = SetCondition.Always,
    expiry: SetExpiry = SetExpiry.Clear
  )(first: (K, V), rest: (K, V)*): F[Boolean] = run(Strings.msetEx(condition, expiry)(first, rest*))

  final def increxBy[K: KeyCodec](
    key: K,
    increment: Long = 1,
    saturate: Boolean = false,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  ): F[IncrExResult[Long]] = run(Strings.increxBy(key, increment, saturate, lowerBound, upperBound, expiry))

  final def increxByFloat[K: KeyCodec](
    key: K,
    increment: Double,
    saturate: Boolean = false,
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    expiry: IncrExpiry = IncrExpiry.Keep
  ): F[IncrExResult[Double]] = run(Strings.increxByFloat(key, increment, saturate, lowerBound, upperBound, expiry))

  final def copy[K: KeyCodec](source: K, destination: K, replace: Boolean = false): F[Boolean] =
    run(Keys.copy(source, destination, replace))

  final def del[K: KeyCodec](first: K, rest: K*): F[Long] = run(Keys.del(first, rest*))

  final def delIfEq[K: KeyCodec, V: ValueCodec](key: K, value: V): F[Boolean] = run(Keys.delIfEq(key, value))

  final def exists[K: KeyCodec](first: K, rest: K*): F[Long] = run(Keys.exists(first, rest*))

  final def expire[K: KeyCodec](key: K, in: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always): F[Boolean] =
    run(Keys.expire(key, in, condition))

  final def expireAt[K: KeyCodec](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always): F[Boolean] =
    run(Keys.expireAt(key, at, condition))

  final def expireTime[K: KeyCodec](key: K): F[ExpiryTime] = run(Keys.expireTime(key))

  final def pExpireTime[K: KeyCodec](key: K): F[ExpiryTime] = run(Keys.pExpireTime(key))

  final def keys[K: KeyCodec](pattern: String): F[Vector[K]] = run(Keys.keys(pattern))

  final def persist[K: KeyCodec](key: K): F[Boolean] = run(Keys.persist(key))

  final def pTtl[K: KeyCodec](key: K): F[Ttl] = run(Keys.pTtl(key))

  final def randomKey[K: KeyCodec]: F[Option[K]] = run(Keys.randomKey)

  final def rename[K: KeyCodec](source: K, destination: K): F[Unit] = run(Keys.rename(source, destination))

  final def renameNx[K: KeyCodec](source: K, destination: K): F[Boolean] = run(Keys.renameNx(source, destination))

  final def scan[K: KeyCodec](
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): F[ScanPage[K]] = run(Keys.scan(cursor, pattern, count, ofType))

  final def touch[K: KeyCodec](first: K, rest: K*): F[Long] = run(Keys.touch(first, rest*))

  final def ttl[K: KeyCodec](key: K): F[Ttl] = run(Keys.ttl(key))

  final def typeOf[K: KeyCodec](key: K): F[Option[RedisType]] = run(Keys.typeOf(key))

  final def unlink[K: KeyCodec](first: K, rest: K*): F[Long] = run(Keys.unlink(first, rest*))

  final def sort[K: KeyCodec, V: ValueCodec](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Vector[Option[V]]] = run(Keys.sort(key, by, limit, get, order, alpha))

  final def sortStore[K: KeyCodec](
    destination: K,
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Long] = run(Keys.sortStore(destination, key, by, limit, get, order, alpha))

  final def sortRo[K: KeyCodec, V: ValueCodec](
    key: K,
    by: Option[String] = None,
    limit: Option[Limit] = None,
    get: Vector[String] = Vector.empty,
    order: SortOrder = SortOrder.Asc,
    alpha: Boolean = false
  ): F[Vector[Option[V]]] = run(Keys.sortRo(key, by, limit, get, order, alpha))

  final def move[K: KeyCodec](key: K, db: Int): F[Boolean] = run(Keys.move(key, db))

  final def dump[K: KeyCodec](key: K): F[Option[Bytes]] = run(Keys.dump(key))

  final def restore[K: KeyCodec](
    key: K,
    payload: Bytes,
    expiry: RestoreExpiry = RestoreExpiry.NoExpiry,
    replace: Boolean = false,
    idleTime: Option[FiniteDuration] = None,
    freq: Option[Long] = None
  ): F[Unit] = run(Keys.restore(key, payload, expiry, replace, idleTime, freq))

  final def migrate[K: KeyCodec](
    host: String,
    port: Int,
    destinationDb: Int,
    timeout: FiniteDuration,
    copy: Boolean = false,
    replace: Boolean = false,
    auth: MigrateAuth = MigrateAuth.None
  )(first: K, rest: K*): F[MigrateResult] = run(Keys.migrate(host, port, destinationDb, timeout, copy, replace, auth)(first, rest*))

  final def objectEncoding[K: KeyCodec](key: K): F[Option[String]] = run(Keys.objectEncoding(key))

  final def objectRefCount[K: KeyCodec](key: K): F[Option[Long]] = run(Keys.objectRefCount(key))

  final def objectFreq[K: KeyCodec](key: K): F[Option[Long]] = run(Keys.objectFreq(key))

  final def objectIdleTime[K: KeyCodec](key: K): F[Option[FiniteDuration]] = run(Keys.objectIdleTime(key))

  final def hSet[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, first: (F0, V), rest: (F0, V)*): F[Long] =
    run(Hashes.hSet(key, first, rest*))

  final def hSetNx[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, field: F0, value: V): F[Boolean] =
    run(Hashes.hSetNx(key, field, value))

  final def hGet[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, field: F0): F[Option[V]] = run(Hashes.hGet(key, field))

  final def hmGet[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, first: F0, rest: F0*): F[Vector[Option[V]]] =
    run(Hashes.hmGet(key, first, rest*))

  final def hDel[K: KeyCodec, F0: KeyCodec](key: K, first: F0, rest: F0*): F[Long] = run(Hashes.hDel(key, first, rest*))

  final def hExists[K: KeyCodec, F0: KeyCodec](key: K, field: F0): F[Boolean] = run(Hashes.hExists(key, field))

  final def hLen[K: KeyCodec](key: K): F[Long] = run(Hashes.hLen(key))

  final def hStrLen[K: KeyCodec, F0: KeyCodec](key: K, field: F0): F[Long] = run(Hashes.hStrLen(key, field))

  final def hKeys[K: KeyCodec, F0: KeyCodec](key: K): F[Vector[F0]] = run(Hashes.hKeys(key))

  final def hVals[K: KeyCodec, V: ValueCodec](key: K): F[Vector[V]] = run(Hashes.hVals(key))

  final def hGetAll[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K): F[Map[F0, V]] = run(Hashes.hGetAll(key))

  final def hIncrBy[K: KeyCodec, F0: KeyCodec](key: K, field: F0, increment: Long): F[Long] =
    run(Hashes.hIncrBy(key, field, increment))

  final def hIncrByFloat[K: KeyCodec, F0: KeyCodec](key: K, field: F0, increment: Double): F[Double] =
    run(Hashes.hIncrByFloat(key, field, increment))

  final def hRandField[K: KeyCodec, F0: KeyCodec](key: K): F[Option[F0]] = run(Hashes.hRandField(key))

  final def hRandField[K: KeyCodec, F0: KeyCodec](key: K, count: Long): F[Vector[F0]] = run(Hashes.hRandField(key, count))

  final def hRandFieldWithValues[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[(F0, V)]] =
    run(Hashes.hRandFieldWithValues(key, count))

  final def hScan[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[(F0, V)]] = run(Hashes.hScan(key, cursor, pattern, count))

  final def hScanNoValues[K: KeyCodec, F0: KeyCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[F0]] = run(Hashes.hScanNoValues(key, cursor, pattern, count))

  final def hExpire[K: KeyCodec, F0: KeyCodec](key: K, ttl: FiniteDuration, condition: ExpireCondition = ExpireCondition.Always)(
    first: F0,
    rest: F0*
  ): F[Vector[FieldExpiry]] = run(Hashes.hExpire(key, ttl, condition)(first, rest*))

  final def hExpireAt[K: KeyCodec, F0: KeyCodec](key: K, at: Instant, condition: ExpireCondition = ExpireCondition.Always)(
    first: F0,
    rest: F0*
  ): F[Vector[FieldExpiry]] = run(Hashes.hExpireAt(key, at, condition)(first, rest*))

  final def hExpireTime[K: KeyCodec, F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldExpiryTime]] =
    run(Hashes.hExpireTime(key)(first, rest*))

  final def hpExpireTime[K: KeyCodec, F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldExpiryTime]] =
    run(Hashes.hpExpireTime(key)(first, rest*))

  final def hTtl[K: KeyCodec, F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldTtl]] =
    run(Hashes.hTtl(key)(first, rest*))

  final def hpTtl[K: KeyCodec, F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldTtl]] =
    run(Hashes.hpTtl(key)(first, rest*))

  final def hPersist[K: KeyCodec, F0: KeyCodec](key: K)(first: F0, rest: F0*): F[Vector[FieldPersist]] =
    run(Hashes.hPersist(key)(first, rest*))

  final def hGetDel[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K)(first: F0, rest: F0*): F[Vector[Option[V]]] =
    run(Hashes.hGetDel(key)(first, rest*))

  final def hGetEx[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, expiry: GetExpiry = GetExpiry.Keep)(
    first: F0,
    rest: F0*
  ): F[Vector[Option[V]]] = run(Hashes.hGetEx(key, expiry)(first, rest*))

  final def hSetEx[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    condition: HSetExCondition = HSetExCondition.Always,
    expiry: SetExpiry = SetExpiry.Clear
  )(first: (F0, V), rest: (F0, V)*): F[Boolean] = run(Hashes.hSetEx(key, condition, expiry)(first, rest*))

  final def lPush[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.lPush(key, first, rest*))

  final def rPush[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.rPush(key, first, rest*))

  final def lPushX[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.lPushX(key, first, rest*))

  final def rPushX[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Lists.rPushX(key, first, rest*))

  final def lPop[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Lists.lPop(key))

  final def rPop[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Lists.rPop(key))

  final def lPopCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Lists.lPopCount(key, count))

  final def rPopCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Lists.rPopCount(key, count))

  final def lLen[K: KeyCodec](key: K): F[Long] = run(Lists.lLen(key))

  final def lRange[K: KeyCodec, V: ValueCodec](key: K, start: Long, stop: Long): F[Vector[V]] = run(Lists.lRange(key, start, stop))

  final def lIndex[K: KeyCodec, V: ValueCodec](key: K, index: Long): F[Option[V]] = run(Lists.lIndex(key, index))

  final def lSet[K: KeyCodec, V: ValueCodec](key: K, index: Long, value: V): F[Unit] = run(Lists.lSet(key, index, value))

  final def lInsert[K: KeyCodec, V: ValueCodec](key: K, position: InsertPosition, pivot: V, value: V): F[Long] =
    run(Lists.lInsert(key, position, pivot, value))

  final def lRem[K: KeyCodec, V: ValueCodec](key: K, count: Long, value: V): F[Long] = run(Lists.lRem(key, count, value))

  final def lTrim[K: KeyCodec](key: K, start: Long, stop: Long): F[Unit] = run(Lists.lTrim(key, start, stop))

  final def lPos[K: KeyCodec, V: ValueCodec](key: K, element: V, rank: Option[Long] = None, maxLen: Option[Long] = None): F[Option[Long]] =
    run(Lists.lPos(key, element, rank, maxLen))

  final def lPosCount[K: KeyCodec, V: ValueCodec](
    key: K,
    element: V,
    count: Long,
    rank: Option[Long] = None,
    maxLen: Option[Long] = None
  ): F[Vector[Long]] = run(Lists.lPosCount(key, element, count, rank, maxLen))

  final def lMove[K: KeyCodec, V: ValueCodec](source: K, destination: K, from: ListSide, to: ListSide): F[Option[V]] =
    run(Lists.lMove(source, destination, from, to))

  final def lMpop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(side: ListSide, count: Option[Long] = None): F[Option[(K, Vector[V])]] =
    run(Lists.lMpop(first, rest*)(side, count))

  final def blPop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V)]] =
    run(Lists.blPop(first, rest*)(timeout))

  final def brPop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V)]] =
    run(Lists.brPop(first, rest*)(timeout))

  final def blMove[K: KeyCodec, V: ValueCodec](source: K, destination: K, from: ListSide, to: ListSide, timeout: BlockTimeout): F[Option[V]] =
    run(Lists.blMove(source, destination, from, to, timeout))

  final def blMpop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    side: ListSide,
    timeout: BlockTimeout,
    count: Option[Long] = None
  ): F[Option[(K, Vector[V])]] = run(Lists.blMpop(first, rest*)(side, timeout, count))

  final def sAdd[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Sets.sAdd(key, first, rest*))

  final def sRem[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Sets.sRem(key, first, rest*))

  final def sCard[K: KeyCodec](key: K): F[Long] = run(Sets.sCard(key))

  final def sIsMember[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Boolean] = run(Sets.sIsMember(key, member))

  final def sMisMember[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Boolean]] =
    run(Sets.sMisMember(key, first, rest*))

  final def sMembers[K: KeyCodec, V: ValueCodec](key: K): F[Set[V]] = run(Sets.sMembers(key))

  final def sMove[K: KeyCodec, V: ValueCodec](source: K, destination: K, member: V): F[Boolean] =
    run(Sets.sMove(source, destination, member))

  final def sPop[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Sets.sPop(key))

  final def sPopCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Set[V]] = run(Sets.sPopCount(key, count))

  final def sRandMember[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(Sets.sRandMember(key))

  final def sRandMemberCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(Sets.sRandMemberCount(key, count))

  final def sDiff[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sDiff(first, rest*))

  final def sDiffStore[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Sets.sDiffStore(destination, first, rest*))

  final def sInter[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sInter(first, rest*))

  final def sInterStore[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Sets.sInterStore(destination, first, rest*))

  final def sInterCard[K: KeyCodec](first: K, rest: K*)(limit: Option[Long] = None): F[Long] = run(Sets.sInterCard(first, rest*)(limit))

  final def sUnion[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Set[V]] = run(Sets.sUnion(first, rest*))

  final def sUnionStore[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Sets.sUnionStore(destination, first, rest*))

  final def sScan[K: KeyCodec, V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[V]] = run(Sets.sScan(key, cursor, pattern, count))

  final def zAdd[K: KeyCodec, V: ValueCodec](key: K, condition: ZAddCondition = ZAddCondition.Always, changed: Boolean = false)(
    first: (V, Double),
    rest: (V, Double)*
  ): F[Long] = run(SortedSets.zAdd(key, condition, changed)(first, rest*))

  final def zAddIncr[K: KeyCodec, V: ValueCodec](
    key: K,
    member: V,
    score: Double,
    condition: ZAddCondition = ZAddCondition.Always
  ): F[Option[Double]] =
    run(SortedSets.zAddIncr(key, member, score, condition))

  final def zCard[K: KeyCodec](key: K): F[Long] = run(SortedSets.zCard(key))

  final def zScore[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Option[Double]] = run(SortedSets.zScore(key, member))

  final def zMScore[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[Double]]] =
    run(SortedSets.zMScore(key, first, rest*))

  final def zIncrBy[K: KeyCodec, V: ValueCodec](key: K, member: V, increment: Double): F[Double] =
    run(SortedSets.zIncrBy(key, member, increment))

  final def zRank[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Option[Long]] = run(SortedSets.zRank(key, member))

  final def zRankWithScore[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Option[(Long, Double)]] =
    run(SortedSets.zRankWithScore(key, member))

  final def zRevRank[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Option[Long]] = run(SortedSets.zRevRank(key, member))

  final def zRevRankWithScore[K: KeyCodec, V: ValueCodec](key: K, member: V): F[Option[(Long, Double)]] =
    run(SortedSets.zRevRankWithScore(key, member))

  final def zCount[K: KeyCodec](key: K, min: ScoreBoundary, max: ScoreBoundary): F[Long] = run(SortedSets.zCount(key, min, max))

  final def zLexCount[K: KeyCodec, V: ValueCodec](key: K, min: LexBoundary[V], max: LexBoundary[V]): F[Long] =
    run(SortedSets.zLexCount(key, min, max))

  final def zRange[K: KeyCodec, V: ValueCodec](key: K, range: ZRange[V]): F[Vector[V]] = run(SortedSets.zRange(key, range))

  final def zRangeWithScores[K: KeyCodec, V: ValueCodec](key: K, range: ZRange[V]): F[Vector[(V, Double)]] =
    run(SortedSets.zRangeWithScores(key, range))

  final def zRangeStore[K: KeyCodec, V: ValueCodec](destination: K, source: K, range: ZRange[V]): F[Long] =
    run(SortedSets.zRangeStore(destination, source, range))

  final def zRem[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(SortedSets.zRem(key, first, rest*))

  final def zRemRangeByRank[K: KeyCodec](key: K, start: Long, stop: Long): F[Long] = run(SortedSets.zRemRangeByRank(key, start, stop))

  final def zRemRangeByScore[K: KeyCodec](key: K, min: ScoreBoundary, max: ScoreBoundary): F[Long] =
    run(SortedSets.zRemRangeByScore(key, min, max))

  final def zRemRangeByLex[K: KeyCodec, V: ValueCodec](key: K, min: LexBoundary[V], max: LexBoundary[V]): F[Long] =
    run(SortedSets.zRemRangeByLex(key, min, max))

  final def zPopMin[K: KeyCodec, V: ValueCodec](key: K): F[Option[(V, Double)]] = run(SortedSets.zPopMin(key))

  final def zPopMax[K: KeyCodec, V: ValueCodec](key: K): F[Option[(V, Double)]] = run(SortedSets.zPopMax(key))

  final def zPopMinCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] = run(SortedSets.zPopMinCount(key, count))

  final def zPopMaxCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] = run(SortedSets.zPopMaxCount(key, count))

  final def zMpop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(minMax: MinMax, count: Option[Long] = None): F[Option[(K, Vector[(V, Double)])]] =
    run(SortedSets.zMpop(first, rest*)(minMax, count))

  final def bzPopMin[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V, Double)]] =
    run(SortedSets.bzPopMin(first, rest*)(timeout))

  final def bzPopMax[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(timeout: BlockTimeout): F[Option[(K, V, Double)]] =
    run(SortedSets.bzPopMax(first, rest*)(timeout))

  final def bzMpop[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    minMax: MinMax,
    timeout: BlockTimeout,
    count: Option[Long] = None
  ): F[Option[(K, Vector[(V, Double)])]] = run(SortedSets.bzMpop(first, rest*)(minMax, timeout, count))

  final def zRandMember[K: KeyCodec, V: ValueCodec](key: K): F[Option[V]] = run(SortedSets.zRandMember(key))

  final def zRandMemberCount[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[V]] = run(SortedSets.zRandMemberCount(key, count))

  final def zRandMemberWithScores[K: KeyCodec, V: ValueCodec](key: K, count: Long): F[Vector[(V, Double)]] =
    run(SortedSets.zRandMemberWithScores(key, count))

  final def zUnion[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[V]] = run(SortedSets.zUnion(first, rest*)(weights, aggregate))

  final def zUnionWithScores[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[(V, Double)]] = run(SortedSets.zUnionWithScores(first, rest*)(weights, aggregate))

  final def zUnionStore[K: KeyCodec](destination: K, first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Long] = run(SortedSets.zUnionStore(destination, first, rest*)(weights, aggregate))

  final def zInter[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[V]] = run(SortedSets.zInter(first, rest*)(weights, aggregate))

  final def zInterWithScores[K: KeyCodec, V: ValueCodec](first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Vector[(V, Double)]] = run(SortedSets.zInterWithScores(first, rest*)(weights, aggregate))

  final def zInterStore[K: KeyCodec](destination: K, first: K, rest: K*)(
    weights: Option[Vector[Double]] = None,
    aggregate: Aggregate = Aggregate.Sum
  ): F[Long] = run(SortedSets.zInterStore(destination, first, rest*)(weights, aggregate))

  final def zInterCard[K: KeyCodec](first: K, rest: K*)(limit: Option[Long] = None): F[Long] =
    run(SortedSets.zInterCard(first, rest*)(limit))

  final def zDiff[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Vector[V]] = run(SortedSets.zDiff(first, rest*))

  final def zDiffWithScores[K: KeyCodec, V: ValueCodec](first: K, rest: K*): F[Vector[(V, Double)]] =
    run(SortedSets.zDiffWithScores(first, rest*))

  final def zDiffStore[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(SortedSets.zDiffStore(destination, first, rest*))

  final def zScan[K: KeyCodec, V: ValueCodec](
    key: K,
    cursor: ScanCursor,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): F[ScanPage[(V, Double)]] = run(SortedSets.zScan(key, cursor, pattern, count))

  final def publish[V: ValueCodec](channel: String, message: V): F[Long] = run(Pubsub.publish(channel, message))

  final def sPublish[V: ValueCodec](channel: String, message: V): F[Long] = run(Pubsub.sPublish(channel, message))

  final def pubsubChannels(pattern: Option[String] = None): F[Vector[String]] = run(Pubsub.pubsubChannels(pattern))

  final def pubsubShardChannels(pattern: Option[String] = None): F[Vector[String]] = run(Pubsub.pubsubShardChannels(pattern))

  final def pubsubNumSub(channels: String*): F[Map[String, Long]] = run(Pubsub.pubsubNumSub(channels*))

  final def pubsubShardNumSub(channels: String*): F[Map[String, Long]] = run(Pubsub.pubsubShardNumSub(channels*))

  final def pubsubNumPat: F[Long] = run(Pubsub.pubsubNumPat)

  final def geoAdd[K: KeyCodec, V: ValueCodec](key: K, condition: GeoAddCondition = GeoAddCondition.Always, changed: Boolean = false)(
    first: (V, GeoCoordinates),
    rest: (V, GeoCoordinates)*
  ): F[Long] = run(Geo.geoAdd(key, condition, changed)(first, rest*))

  final def geoDist[K: KeyCodec, V: ValueCodec](key: K, member1: V, member2: V, unit: GeoUnit = GeoUnit.Meters): F[Option[Double]] =
    run(Geo.geoDist(key, member1, member2, unit))

  final def geoHash[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[String]]] =
    run(Geo.geoHash(key, first, rest*))

  final def geoPos[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Vector[Option[GeoCoordinates]]] =
    run(Geo.geoPos(key, first, rest*))

  final def geoSearch[K: KeyCodec, V: ValueCodec](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  ): F[Vector[V]] = run(Geo.geoSearch(key, origin, shape, sort, count))

  final def geoSearchWith[K: KeyCodec, V: ValueCodec](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    withCoord: Boolean = false,
    withDist: Boolean = false,
    withHash: Boolean = false,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  ): F[Vector[GeoSearchResult[V]]] = run(Geo.geoSearchWith(key, origin, shape, withCoord, withDist, withHash, sort, count))

  final def geoSearchStore[K: KeyCodec, V: ValueCodec](
    destination: K,
    source: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None,
    storeDist: Boolean = false
  ): F[Long] = run(Geo.geoSearchStore(destination, source, origin, shape, sort, count, storeDist))

  final def setBit[K: KeyCodec](key: K, offset: Long, value: Boolean): F[Boolean] = run(Bitmaps.setBit(key, offset, value))

  final def getBit[K: KeyCodec](key: K, offset: Long): F[Boolean] = run(Bitmaps.getBit(key, offset))

  final def bitCount[K: KeyCodec](key: K, range: Option[BitRange] = None): F[Long] = run(Bitmaps.bitCount(key, range))

  final def bitPos[K: KeyCodec](key: K, bit: Boolean, range: Option[BitPosRange] = None): F[Long] = run(Bitmaps.bitPos(key, bit, range))

  final def bitOpAnd[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpAnd(destination, first, rest*))

  final def bitOpOr[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpOr(destination, first, rest*))

  final def bitOpXor[K: KeyCodec](destination: K, first: K, rest: K*): F[Long] = run(Bitmaps.bitOpXor(destination, first, rest*))

  final def bitOpNot[K: KeyCodec](destination: K, source: K): F[Long] = run(Bitmaps.bitOpNot(destination, source))

  final def bitField[K: KeyCodec](key: K, first: BitFieldOp, rest: BitFieldOp*): F[Vector[Option[Long]]] =
    run(Bitmaps.bitField(key, first, rest*))

  final def bitFieldRo[K: KeyCodec](key: K, first: BitFieldOp.Get, rest: BitFieldOp.Get*): F[Vector[Long]] =
    run(Bitmaps.bitFieldRo(key, first, rest*))

  final def pfAdd[K: KeyCodec, V: ValueCodec](key: K, elements: V*): F[Boolean] = run(HyperLogLog.pfAdd(key, elements*))

  final def pfCount[K: KeyCodec](first: K, rest: K*): F[Long] = run(HyperLogLog.pfCount(first, rest*))

  final def pfMerge[K: KeyCodec](destination: K, sources: K*): F[Unit] = run(HyperLogLog.pfMerge(destination, sources*))

  final def xAdd[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    id: XAddId = XAddId.Auto,
    trim: Option[Trimming] = None,
    policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef
  )(first: (F0, V), rest: (F0, V)*): F[StreamId] = run(Streams.xAdd(key, id, trim, policy)(first, rest*))

  final def xAddNoMkStream[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    id: XAddId = XAddId.Auto,
    trim: Option[Trimming] = None,
    policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef
  )(first: (F0, V), rest: (F0, V)*): F[Option[StreamId]] = run(Streams.xAddNoMkStream(key, id, trim, policy)(first, rest*))

  final def xLen[K: KeyCodec](key: K): F[Long] = run(Streams.xLen(key))

  final def xDel[K: KeyCodec](key: K)(first: StreamId, rest: StreamId*): F[Long] = run(Streams.xDel(key)(first, rest*))

  final def xTrim[K: KeyCodec](key: K, trim: Trimming, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef): F[Long] =
    run(Streams.xTrim(key, trim, policy))

  final def xSetId[K: KeyCodec](key: K, id: GroupStartId, entriesAdded: Option[Long] = None, maxDeletedId: Option[StreamId] = None): F[Unit] =
    run(Streams.xSetId(key, id, entriesAdded, maxDeletedId))

  final def xCfgSet[K: KeyCodec](key: K, idmpDuration: Option[FiniteDuration] = None, idmpMaxSize: Option[Long] = None): F[Unit] =
    run(Streams.xCfgSet(key, idmpDuration, idmpMaxSize))

  final def xRange[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    count: Option[Long] = None
  ): F[Vector[StreamEntry[F0, V]]] = run(Streams.xRange(key, start, end, count))

  final def xRevRange[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    end: StreamRangeId = StreamRangeId.Max,
    start: StreamRangeId = StreamRangeId.Min,
    count: Option[Long] = None
  ): F[Vector[StreamEntry[F0, V]]] = run(Streams.xRevRange(key, end, start, count))

  final def xRead[K: KeyCodec, F0: KeyCodec, V: ValueCodec](first: (K, ReadId), rest: (K, ReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None
  ): F[Vector[(K, Vector[StreamEntry[F0, V]])]] = run(Streams.xRead(first, rest*)(count, block))

  final def xReadGroup[K: KeyCodec, F0: KeyCodec, V: ValueCodec](group: String, consumer: String)(first: (K, GroupReadId), rest: (K, GroupReadId)*)(
    count: Option[Long] = None,
    block: Option[BlockTimeout] = None,
    noAck: Boolean = false
  ): F[Vector[(K, Vector[StreamEntry[F0, V]])]] = run(Streams.xReadGroup(group, consumer)(first, rest*)(count, block, noAck))

  final def xAck[K: KeyCodec](key: K, group: String)(first: StreamId, rest: StreamId*): F[Long] = run(Streams.xAck(key, group)(first, rest*))

  final def xGroupCreate[K: KeyCodec](
    key: K,
    group: String,
    id: GroupStartId = GroupStartId.Last,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): F[Unit] = run(Streams.xGroupCreate(key, group, id, mkStream, entriesRead))

  final def xGroupSetId[K: KeyCodec](key: K, group: String, id: GroupStartId = GroupStartId.Last, entriesRead: Option[Long] = None): F[Unit] =
    run(Streams.xGroupSetId(key, group, id, entriesRead))

  final def xGroupDestroy[K: KeyCodec](key: K, group: String): F[Boolean] = run(Streams.xGroupDestroy(key, group))

  final def xGroupCreateConsumer[K: KeyCodec](key: K, group: String, consumer: String): F[Boolean] =
    run(Streams.xGroupCreateConsumer(key, group, consumer))

  final def xGroupDelConsumer[K: KeyCodec](key: K, group: String, consumer: String): F[Long] =
    run(Streams.xGroupDelConsumer(key, group, consumer))

  final def xClaim[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, group: String, consumer: String, minIdle: FiniteDuration)(
    first: StreamId,
    rest: StreamId*
  )(idle: Option[ClaimIdle] = None, retryCount: Option[Long] = None, force: Boolean = false): F[Vector[StreamEntry[F0, V]]] =
    run(Streams.xClaim(key, group, consumer, minIdle)(first, rest*)(idle, retryCount, force))

  final def xClaimJustId[K: KeyCodec](key: K, group: String, consumer: String, minIdle: FiniteDuration)(first: StreamId, rest: StreamId*)(
    idle: Option[ClaimIdle] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): F[Vector[StreamId]] = run(Streams.xClaimJustId(key, group, consumer, minIdle)(first, rest*)(idle, retryCount, force))

  final def xAutoClaim[K: KeyCodec, F0: KeyCodec, V: ValueCodec](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  ): F[XAutoClaimResult[F0, V]] = run(Streams.xAutoClaim(key, group, consumer, minIdle, start, count))

  final def xAutoClaimJustId[K: KeyCodec](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  ): F[XAutoClaimJustIdResult] = run(Streams.xAutoClaimJustId(key, group, consumer, minIdle, start, count))

  final def xPending[K: KeyCodec](key: K, group: String): F[PendingSummary] = run(Streams.xPending(key, group))

  final def xPendingExtended[K: KeyCodec](
    key: K,
    group: String,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    count: Long = 10L,
    consumer: Option[String] = None,
    idle: Option[FiniteDuration] = None
  ): F[Vector[PendingEntry]] = run(Streams.xPendingExtended(key, group, start, end, count, consumer, idle))

  final def xInfoStream[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K): F[StreamInfo[F0, V]] = run(StreamInfo.xInfoStream(key))

  final def xInfoStreamFull[K: KeyCodec, F0: KeyCodec, V: ValueCodec](key: K, count: Option[Long] = None): F[StreamInfoFull[F0, V]] =
    run(StreamInfo.xInfoStreamFull(key, count))

  final def xInfoGroups[K: KeyCodec](key: K): F[Vector[GroupInfo]] = run(StreamInfo.xInfoGroups(key))

  final def xInfoConsumers[K: KeyCodec](key: K, group: String): F[Vector[ConsumerInfo]] = run(StreamInfo.xInfoConsumers(key, group))

  final def xDelEx[K: KeyCodec](key: K, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: StreamId,
    rest: StreamId*
  ): F[Vector[StreamEntryDeletion]] =
    run(Streams.xDelEx(key, policy)(first, rest*))

  final def xAckDel[K: KeyCodec](key: K, group: String, policy: StreamDeletionPolicy = StreamDeletionPolicy.KeepRef)(
    first: StreamId,
    rest: StreamId*
  ): F[Vector[StreamEntryDeletion]] = run(Streams.xAckDel(key, group, policy)(first, rest*))

  final def xNack[K: KeyCodec](key: K, group: String, mode: NackMode)(first: StreamId, rest: StreamId*)(
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): F[Long] = run(Streams.xNack(key, group, mode)(first, rest*)(retryCount, force))

  // --- scripting -----------------------------------------------------------------------------------------------------------------------

  final def eval(script: String): F[Frame]                                                         = run(Scripting.eval(script))
  final def eval[K: KeyCodec](script: String, keys: Seq[K]): F[Frame]                              = run(Scripting.eval(script, keys))
  final def eval[K: KeyCodec, V: ValueCodec](script: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.eval(script, keys, args))

  final def evalRo(script: String): F[Frame]                                                         = run(Scripting.evalRo(script))
  final def evalRo[K: KeyCodec](script: String, keys: Seq[K]): F[Frame]                              = run(Scripting.evalRo(script, keys))
  final def evalRo[K: KeyCodec, V: ValueCodec](script: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalRo(script, keys, args))

  final def evalSha(sha: String): F[Frame]                                                         = run(Scripting.evalSha(sha))
  final def evalSha[K: KeyCodec](sha: String, keys: Seq[K]): F[Frame]                              = run(Scripting.evalSha(sha, keys))
  final def evalSha[K: KeyCodec, V: ValueCodec](sha: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalSha(sha, keys, args))

  final def evalShaRo(sha: String): F[Frame]                                                         = run(Scripting.evalShaRo(sha))
  final def evalShaRo[K: KeyCodec](sha: String, keys: Seq[K]): F[Frame]                              = run(Scripting.evalShaRo(sha, keys))
  final def evalShaRo[K: KeyCodec, V: ValueCodec](sha: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Scripting.evalShaRo(sha, keys, args))

  final def scriptLoad(script: String): F[String]                          = run(Scripting.scriptLoad(script))
  final def scriptExists(first: String, rest: String*): F[Vector[Boolean]] = run(Scripting.scriptExists(first, rest*))
  final def scriptFlush(mode: Option[FlushMode] = None): F[Unit]           = run(Scripting.scriptFlush(mode))
  final def scriptKill: F[Unit]                                            = run(Scripting.scriptKill)
  final def scriptShow(sha: String): F[String]                             = run(Scripting.scriptShow(sha))

  // --- functions -----------------------------------------------------------------------------------------------------------------------

  final def fCall(function: String): F[Frame]                                                         = run(Functions.fCall(function))
  final def fCall[K: KeyCodec](function: String, keys: Seq[K]): F[Frame]                              = run(Functions.fCall(function, keys))
  final def fCall[K: KeyCodec, V: ValueCodec](function: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Functions.fCall(function, keys, args))

  final def fCallRo(function: String): F[Frame]                                                         = run(Functions.fCallRo(function))
  final def fCallRo[K: KeyCodec](function: String, keys: Seq[K]): F[Frame]                              = run(Functions.fCallRo(function, keys))
  final def fCallRo[K: KeyCodec, V: ValueCodec](function: String, keys: Seq[K], args: Seq[V]): F[Frame] =
    run(Functions.fCallRo(function, keys, args))

  final def functionLoad(code: String, replace: Boolean = false): F[String]                                     = run(Functions.functionLoad(code, replace))
  final def functionDelete(libraryName: String): F[Unit]                                                        = run(Functions.functionDelete(libraryName))
  final def functionFlush(mode: Option[FlushMode] = None): F[Unit]                                              = run(Functions.functionFlush(mode))
  final def functionKill: F[Unit]                                                                               = run(Functions.functionKill)
  final def functionDump: F[Bytes]                                                                              = run(Functions.functionDump)
  final def functionRestore(payload: Bytes, policy: Option[RestorePolicy] = None): F[Unit]                      =
    run(Functions.functionRestore(payload, policy))
  final def functionList(libraryName: Option[String] = None, withCode: Boolean = false): F[Vector[LibraryInfo]] =
    run(Functions.functionList(libraryName, withCode))
  final def functionStats: F[FunctionStats]                                                                     = run(Functions.functionStats)

  // --- server admin --------------------------------------------------------------------------------------------------------------------

  final def configGet(parameter: String, rest: String*): F[Map[String, String]]                      = run(Server.configGet(parameter, rest*))
  final def configSet(setting: (String, String), rest: (String, String)*): F[Unit]                   = run(Server.configSet(setting, rest*))
  final def info(sections: String*): F[String]                                                       = run(Server.info(sections*))
  final def dbSize: F[Long]                                                                          = run(Server.dbSize)
  final def time: F[Instant]                                                                         = run(Server.time)
  final def role: F[Role]                                                                            = run(Server.role)
  final def flushAll(mode: Option[FlushMode] = None): F[Unit]                                        = run(Server.flushAll(mode))
  final def flushDb(mode: Option[FlushMode] = None): F[Unit]                                         = run(Server.flushDb(mode))
  final def waitReplicas(numReplicas: Long, timeout: FiniteDuration): F[Long]                        = run(Server.waitReplicas(numReplicas, timeout))
  final def waitAof(numLocal: Long, numReplicas: Long, timeout: FiniteDuration): F[(Long, Long)]     =
    run(Server.waitAof(numLocal, numReplicas, timeout))
  final def memoryUsage[K: KeyCodec](key: K, samples: Option[Long] = None): F[Option[Long]]          = run(Server.memoryUsage(key, samples))
  final def memoryPurge: F[Unit]                                                                     = run(Server.memoryPurge)
  final def slowLogGet(count: Option[Long] = None): F[Vector[SlowLogEntry]]                          = run(Server.slowLogGet(count))
  final def slowLogLen: F[Long]                                                                      = run(Server.slowLogLen)
  final def slowLogReset: F[Unit]                                                                    = run(Server.slowLogReset)
  final def commandLogGet(count: Long, logType: CommandLogType): F[Vector[CommandLogEntry]]          = run(Server.commandLogGet(count, logType))
  final def commandLogLen(logType: CommandLogType): F[Long]                                          = run(Server.commandLogLen(logType))
  final def commandLogReset(logType: CommandLogType): F[Unit]                                        = run(Server.commandLogReset(logType))
  final def latencyHistory(event: String): F[Vector[(Instant, FiniteDuration)]]                      = run(Server.latencyHistory(event))
  final def latencyLatest: F[Vector[LatencyEntry]]                                                   = run(Server.latencyLatest)
  final def latencyReset(events: String*): F[Long]                                                   = run(Server.latencyReset(events*))
  final def latencyHistogram(commands: String*): F[Map[String, CommandHistogram]]                    = run(Server.latencyHistogram(commands*))
  final def commandCount: F[Long]                                                                    = run(Server.commandCount)
  final def commandList(filterBy: Option[CommandFilterBy] = None): F[Vector[String]]                 = run(Server.commandList(filterBy))
  final def commandGetKeys(command: String, args: String*): F[Vector[String]]                        = run(Server.commandGetKeys(command, args*))
  final def commandGetKeysAndFlags(command: String, args: String*): F[Vector[(String, Set[String])]] =
    run(Server.commandGetKeysAndFlags(command, args*))
  final def commandInfo(commands: String*): F[Vector[CommandInfo]]                                   = run(Server.commandInfo(commands*))
  final def clusterInfo: F[String]                                                                   = run(Server.clusterInfo)
  final def clusterNodes: F[String]                                                                  = run(Server.clusterNodes)
  final def clusterMyId: F[String]                                                                   = run(Server.clusterMyId)
  final def clusterKeySlot(key: String): F[Long]                                                     = run(Server.clusterKeySlot(key))
  final def clusterCountKeysInSlot(slot: Int): F[Long]                                               = run(Server.clusterCountKeysInSlot(slot))

  // --- access control ------------------------------------------------------------------------------------------------------------------

  final def aclWhoAmI: F[String]                                       = run(Acl.aclWhoAmI)
  final def aclList: F[Vector[String]]                                 = run(Acl.aclList)
  final def aclUsers: F[Vector[String]]                                = run(Acl.aclUsers)
  final def aclCat(category: Option[String] = None): F[Vector[String]] = run(Acl.aclCat(category))
  final def aclGetUser(username: String): F[Option[AclUser]]           = run(Acl.aclGetUser(username))
  final def aclLog(count: Option[Long] = None): F[Vector[AclLogEntry]] = run(Acl.aclLog(count))

  // --- connection ----------------------------------------------------------------------------------------------------------------------

  final def echo(message: String): F[String] = run(Connection.echo(message))
  final def clientId: F[Long]                = run(Connection.clientId)
  final def clientGetName: F[String]         = run(Connection.clientGetName)
  final def clientInfo: F[String]            = run(Connection.clientInfo)
  final def clientList: F[String]            = run(Connection.clientList)
  final def clientGetRedir: F[Long]          = run(Connection.clientGetRedir)

  // --- arrays --------------------------------------------------------------------------------------------------------------------------

  final def arSet[K: KeyCodec, V: ValueCodec](key: K, index: Long, first: V, rest: V*): F[Long] = run(Arrays.arSet(key, index, first, rest*))

  final def arMSet[K: KeyCodec, V: ValueCodec](key: K, first: (Long, V), rest: (Long, V)*): F[Long] = run(Arrays.arMSet(key, first, rest*))

  final def arGet[K: KeyCodec, V: ValueCodec](key: K, index: Long): F[Option[V]] = run(Arrays.arGet(key, index))

  final def arMGet[K: KeyCodec, V: ValueCodec](key: K, first: Long, rest: Long*): F[Vector[Option[V]]] = run(Arrays.arMGet(key, first, rest*))

  final def arLen[K: KeyCodec](key: K): F[Long] = run(Arrays.arLen(key))

  final def arCount[K: KeyCodec](key: K): F[Long] = run(Arrays.arCount(key))

  final def arGetRange[K: KeyCodec, V: ValueCodec](key: K, start: Long, end: Long): F[Vector[Option[V]]] =
    run(Arrays.arGetRange(key, start, end))

  final def arRing[K: KeyCodec, V: ValueCodec](key: K, size: Long, first: V, rest: V*): F[Long] = run(Arrays.arRing(key, size, first, rest*))

  final def arLastItems[K: KeyCodec, V: ValueCodec](key: K, count: Long, rev: Boolean = false): F[Vector[V]] =
    run(Arrays.arLastItems(key, count, rev))

  final def arDel[K: KeyCodec](key: K, first: Long, rest: Long*): F[Long] = run(Arrays.arDel(key, first, rest*))

  final def arDelRange[K: KeyCodec](key: K, first: (Long, Long), rest: (Long, Long)*): F[Long] = run(Arrays.arDelRange(key, first, rest*))

  final def arInsert[K: KeyCodec, V: ValueCodec](key: K, first: V, rest: V*): F[Long] = run(Arrays.arInsert(key, first, rest*))

  final def arNext[K: KeyCodec](key: K): F[Option[Long]] = run(Arrays.arNext(key))

  final def arSeek[K: KeyCodec](key: K, index: Long): F[Boolean] = run(Arrays.arSeek(key, index))

  final def arScan[K: KeyCodec, V: ValueCodec](key: K, start: Long, end: Long, limit: Option[Long] = None): F[Vector[(Long, V)]] =
    run(Arrays.arScan(key, start, end, limit))

  final def arGrep[K: KeyCodec](
    key: K,
    start: Long,
    end: Long,
    combine: ArGrepCombine = ArGrepCombine.Or,
    limit: Option[Long] = None,
    noCase: Boolean = false
  )(first: ArMatch, rest: ArMatch*): F[Vector[Long]] = run(Arrays.arGrep(key, start, end, combine, limit, noCase)(first, rest*))

  final def arGrepWithValues[K: KeyCodec, V: ValueCodec](
    key: K,
    start: Long,
    end: Long,
    combine: ArGrepCombine = ArGrepCombine.Or,
    limit: Option[Long] = None,
    noCase: Boolean = false
  )(first: ArMatch, rest: ArMatch*): F[Vector[(Long, V)]] = run(Arrays.arGrepWithValues(key, start, end, combine, limit, noCase)(first, rest*))

  final def arOpSum[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpSum(key, start, end))
  final def arOpMin[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpMin(key, start, end))
  final def arOpMax[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Double]] = run(Arrays.arOpMax(key, start, end))
  final def arOpAnd[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Long]]   = run(Arrays.arOpAnd(key, start, end))
  final def arOpOr[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Long]]    = run(Arrays.arOpOr(key, start, end))
  final def arOpXor[K: KeyCodec](key: K, start: Long, end: Long): F[Option[Long]]   = run(Arrays.arOpXor(key, start, end))
  final def arOpUsed[K: KeyCodec](key: K, start: Long, end: Long): F[Long]          = run(Arrays.arOpUsed(key, start, end))

  final def arOpMatch[K: KeyCodec, V: ValueCodec](key: K, start: Long, end: Long, value: V): F[Long] =
    run(Arrays.arOpMatch(key, start, end, value))

  final def arInfo[K: KeyCodec](key: K): F[ArrayInfo] = run(Arrays.arInfo(key))

  final def arInfoFull[K: KeyCodec](key: K): F[ArrayInfoFull] = run(Arrays.arInfoFull(key))
}

/**
  * The user-facing handle owning all connections to one server: the command surface, plus pipelines and transactions. Commands compose into
  * a [[Pipeline]] value (built from [[sage.commands.Commands]]) that `pipeline` sends in one round-trip, yielding a typed result per command.
  */
// one independent keyspace a full SCAN sweep visits: a single node standalone, one per slot-owning master in cluster. `node` is None when
// the backend has no node to pin to (standalone, or a not-yet-discovered cluster topology), in which case the command runs through normal routing.
final private[sage] case class ScanTarget(node: Option[Node])

private[sage] object ScanTarget {
  val any: ScanTarget = ScanTarget(None)
}

// drives the cluster-wide SCAN walk: scan each target to its node-local zero cursor, then move to the next, so the iteration never silently
// covers a single node. Begin resolves the targets, Visit pages the head target (resetting the cursor on each hand-off), End stops the stream.
private[sage] enum ScanStep {
  case Begin
  case Visit(cursor: ScanCursor, remaining: Vector[ScanTarget])
  case End
}

trait Client[F[_]] extends CommandRunner[F] {

  /**
    * Runs a read with client-side caching: served from the local cache until a server invalidation push or `ttl` evicts it. Only a
    * cacheable command with at least one key qualifies — a read whose result is a pure function of its keys' state, so an invalidation
    * covers every change. A write, a keyless read, or a time-varying/non-deterministic read (`TTL`, `SRANDMEMBER`) fails with
    * [[sage.SageException.NotCacheable]]. On a cluster client this currently runs the read without caching, so the same call stays
    * topology-portable.
    */
  def cached[A](command: Command[A], ttl: FiniteDuration): F[A]

  def pipeline[Out, R](p: Pipeline[Out, R]): F[Out]

  def pipelineAttempt[Out, R](p: Pipeline[Out, R]): F[R]

  def transaction[A](body: TransactionScope[F] => F[A]): F[A]

  // the pub/sub seam: returns a handle each backend wraps into its native stream; the ergonomic `subscribe`/`pSubscribe`/`sSubscribe` are facades
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]]

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): F[Subscription[F, PatternMessage[V]]]

  // Shard Channel subscriptions: in a cluster each channel is routed to the Node owning its Slot; a sharded delivery is an ordinary Message
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]]

  // the keyspaces a full SCAN sweep must visit (one per slot-owning master in cluster), and a node-pinned run for the keyless SCAN page so a
  // cursor resumes on the node it came from. The streaming `scanAll` walks the targets in order; keyed h/s/zScan stay single-target and route by key.
  private[sage] def scanTargets: F[Vector[ScanTarget]]

  private[sage] def runOn[A](target: ScanTarget, command: Command[A]): F[A]

  def close: F[Unit]
}

object Client {

  private val defaults = SageConfig()

  // a cacheable read's result is a pure function of its keys' state (Command.cacheable) and it names at least one key — a keyless read
  // could only ever be evicted by TTL, never by an invalidation push, so it is rejected rather than allowed to silently go stale
  private[internal] def cacheable(command: Command[?]): Boolean = command.cacheable && command.keyIndices.nonEmpty

  private[internal] def notCacheable(command: Command[?]): NotCacheable =
    NotCacheable(s"${command.name} is not cacheable: cached requires a cacheable command with at least one key")

  def connect(config: SageConfig): CIO[Client[CIO]] =
    validate(config) match {
      case Some(problem) => CIO.fail(new IllegalArgumentException(problem))
      case None          =>
        config.topology match {
          case Topology.Standalone                    => connectStandalone(config)
          case Topology.Cluster(seeds, clusterConfig) =>
            ClusterLive.connect(config, seeds.map(e => Node(e.host, e.port)), clusterConfig, Scheduler.real, translateHandshake)
        }
    }

  // a misconfigured client is a programmer error, surfaced through the connect effect (never thrown from a constructor) and deliberately
  // outside the sealed hierarchy, like the other usage guards
  private def validate(config: SageConfig): Option[String] = {
    // pingInterval/pingTimeout are inert when the watchdog is disabled, so don't reject them then
    val watchdog =
      if (config.watchdog.enabled)
        Vector(
          positive(config.watchdog.pingInterval, "watchdog.pingInterval"),
          positive(config.watchdog.pingTimeout, "watchdog.pingTimeout")
        )
      else Vector.empty
    val checks   = Vector(
      port(config.port, "port"),
      positive(config.connectTimeout, "connectTimeout"),
      positive(config.closeTimeout, "closeTimeout"),
      positive(config.reconnect.initialDelay, "reconnect.initialDelay"),
      cond(config.reconnect.maxDelay >= config.reconnect.initialDelay, "reconnect.maxDelay must be >= initialDelay"),
      cond(config.reconnect.multiplier >= 1.0, "reconnect.multiplier must be >= 1.0"),
      cond(config.dedicatedPool.maxConnections >= 1, "dedicatedPool.maxConnections must be >= 1"),
      positive(config.dedicatedPool.acquireTimeout, "dedicatedPool.acquireTimeout"),
      cond(config.pubsub.bufferSize >= 1, "pubsub.bufferSize must be >= 1")
    ) ++ watchdog ++ (config.topology match {
      case Topology.Cluster(seeds, cluster) =>
        Vector(
          cond(seeds.nonEmpty, "cluster topology requires at least one seed"),
          cond(cluster.maxRedirects >= 0, "cluster.maxRedirects must be >= 0"),
          positive(cluster.minRefreshInterval, "cluster.minRefreshInterval")
        ) ++ seeds.map(s => port(s.port, s"seed ${s.host}:${s.port} port"))
      case Topology.Standalone              => Vector.empty
    })
    checks.flatten.headOption
  }

  private def cond(ok: Boolean, problem: String): Option[String]             = if (ok) None else Some(problem)
  private def port(value: Int, label: String): Option[String]                = cond(value >= 1 && value <= 65535, s"$label must be in 1..65535")
  private def positive(value: FiniteDuration, label: String): Option[String] = cond(value.toNanos > 0L, s"$label must be positive")

  private def connectStandalone(config: SageConfig): CIO[Client[CIO]] =
    // build the TLS context once (eager failure on bad trust material), then capture it in the reconnect factory so every connection — the
    // multiplexed one and each dedicated one — is upgraded identically
    CIO.blocking(Tls.buildUpgrade(config.tls, config.host, config.port)).flatMap { upgrade =>
      connectWith(
        (onFrame, onClosed) => SocketTransport.connect(config.host, config.port, config.connectTimeout, upgrade, onFrame, onClosed),
        Scheduler.real,
        config.reconnect,
        config.watchdog,
        config.connectTimeout,
        config.closeTimeout,
        config.dedicatedPool,
        config.pubsub,
        config.auth,
        config.clientCache.maxBytes,
        config.clientCache.enabled
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
    cachingEnabled: Boolean = defaults.clientCache.enabled
  ): CIO[Client[CIO]] = {
    val bootstrap            = Vector(Connection.hello(auth.map(a => a.username -> a.password)))
    // only the Multiplexed Connection caches reads, so only it enables tracking; the dedicated pool and subscription connection keep the
    // plain HELLO bootstrap. Tracking is skipped entirely when caching is disabled, so a server that denies CLIENT TRACKING still connects.
    val multiplexedBootstrap = if (cachingEnabled) bootstrap :+ Connection.clientTrackingOnOptin else bootstrap
    CIO
      .blocking(
        MultiplexedConnection.connect(factory, scheduler, multiplexedBootstrap, reconnect, watchdog, connectTimeout, closeTimeout, cacheMaxBytes)
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
          () => connection.isLive
        )
        new Live(connection, pool, subscriptions, cachingEnabled)
      }
      .mapError(translateHandshake)
  }

  // pre-6.0 Redis answers HELLO with an unknown-command error; newer servers reject an unsupported protocol version with NOPROTO. A
  // rejected certificate or hostname mismatch surfaces from the handshake as an SSLException, which would otherwise escape the sealed
  // hierarchy.
  private def translateHandshake(error: Throwable): Throwable =
    error match {
      case ServerError(message) if message.startsWith("NOPROTO") || message.toLowerCase.contains("unknown command") =>
        UnsupportedServer(s"sage requires RESP3 (Redis 6.0+ or any Valkey); server rejected HELLO 3: $message")
      case e: SSLException                                                                                          =>
        TlsError(s"TLS handshake failed: ${e.getMessage}")
      case other                                                                                                    => other
    }

  final private class Live(
    connection: MultiplexedConnection,
    pool: DedicatedPool,
    subscriptions: SubscriptionConnection,
    cachingEnabled: Boolean
  ) extends Client[CIO] {

    def run[A](command: Command[A]): CIO[A] =
      command.execution match {
        case Execution.Ordinary => CIO.async(callback => connection.submit(command, callback))
        case Execution.Blocking => CIO.async(callback => pool.use(command, callback))
      }

    def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
      if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
      else if (!cachingEnabled) run(command) // tracking was never enabled, so run uncached rather than issue an unbacked CLIENT CACHING YES
      else CIO.async(callback => connection.cachedSubmit(command, ttl.toMillis, callback))

    def scanTargets: CIO[Vector[ScanTarget]] = CIO.value(Vector(ScanTarget.any))

    def runOn[A](target: ScanTarget, command: Command[A]): CIO[A] = run(command)

    def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out] =
      submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] =
      submitPipeline(p).map(p.toResults)

    // The lease is bracketed: release runs on success, failure, and interruption (IO.bracket). A clean exit (exec/discard cleared the
    // connection's WATCH/MULTI state, no reply outstanding) recycles the connection; a scope left armed or interrupted mid-command is
    // discarded rather than handed to the next borrower dirty.
    def transaction[A](body: TransactionScope[CIO] => CIO[A]): CIO[A] =
      CIO.acquireReleaseWith(acquireScope)(releaseScope)(scope => body(scope))

    private def acquireScope: CIO[TxScope] =
      CIO.blocking {
        try new TxScope(pool.acquireForTransaction())
        catch {
          case e: NotConnected => throw e
          case e: TimedOut     => throw e
          case NonFatal(_)     => throw ConnectionLost(mayHaveExecuted = false)
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
          val collector = new TxSupport.IndexedCollector[Either[SageException, Any]](p.commands.length, complete)
          val callbacks = Vector.tabulate(p.commands.length)(i => (result: Try[Any]) => collector.set(i, TxSupport.toEither(result)))
          if (!connection.submitAll(p.commands, callbacks)) complete(Failure(NotConnected()))
        }

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
      CIO.blocking(channelMessages(subscriptions.subscribeChannels(channel +: rest.toVector)))

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
      CIO.blocking(patternMessages(subscriptions.subscribePatterns(pattern +: rest.toVector)))

    // a standalone server has no slots, so every Shard Channel rides the one Subscription Connection in shard mode
    def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
      CIO.blocking(channelMessages(subscriptions.subscribeShard(channel +: rest.toVector)))

    def close: CIO[Unit] = CIO.blocking { subscriptions.close(); pool.close(); connection.close() }
  }

  // wrap a raw subscription as the effect-typed seam each backend lowers into its native stream; a channel/shard delivery is a Message
  private[internal] def channelMessages[V](raw: SubscriptionConnection.RawSubscription)(using ValueCodec[V]): Subscription[CIO, Message[V]] =
    new Subscription[CIO, Message[V]] {
      // async, not blocking: the reader thread completes the callback, so a fiber parks instead of pinning a runtime worker
      def next: CIO[Option[Message[V]]] = CIO.async { complete =>
        raw.next {
          case Some(SubscriptionConnection.Delivery.Channel(ch, payload)) => complete(Try(Some(Message(ch, decodeOrThrow[V](payload)))))
          case _                                                          => complete(Success(None))
        }
      }
      def close: CIO[Unit]              = CIO.blocking(raw.close())
    }

  private[internal] def patternMessages[V](raw: SubscriptionConnection.RawSubscription)(using ValueCodec[V]): Subscription[CIO, PatternMessage[V]] =
    new Subscription[CIO, PatternMessage[V]] {
      def next: CIO[Option[PatternMessage[V]]] = CIO.async { complete =>
        raw.next {
          case Some(SubscriptionConnection.Delivery.Pattern(pat, ch, payload)) =>
            complete(Try(Some(PatternMessage(pat, ch, decodeOrThrow[V](payload)))))
          case _                                                               => complete(Success(None))
        }
      }
      def close: CIO[Unit]                     = CIO.blocking(raw.close())
    }

  // an undecodable payload fails the subscription stream rather than being silently dropped
  private def decodeOrThrow[V](payload: sage.Bytes)(using codec: ValueCodec[V]): V =
    codec.decode(payload) match {
      case Right(value) => value
      case Left(error)  => throw error
    }

  final private class TxScope(val conn: DedicatedConnection) extends TransactionScope[CIO] {

    // tracks whether watched keys may still be armed on the connection; set as soon as WATCH is attempted, cleared by EXEC/UNWATCH
    val armed = new AtomicBoolean(false)

    // The lock makes "reject if released, else submit" atomic with the finalizer's seal-and-decide ([[sealAndReusable]]): a command
    // submitted under it is in-flight before the finalizer reads quiescence, so a handle captured past the block and raced against release
    // either submits onto a connection the finalizer then declines to recycle, or is rejected outright — never onto a re-borrowed one.
    private val lock     = new ReentrantLock()
    private var released = false

    private def submitting[A](complete: Try[A] => Unit)(submit: => Unit): Unit = {
      lock.lock()
      try if (released) complete(Failure(TxSupport.scopeReleasedError)) else submit
      finally lock.unlock()
    }

    // run once by the lease finalizer: seals the scope against further operations and reports whether the connection may be recycled
    private[Client] def sealAndReusable(): Boolean = {
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
        submitting(complete) {
          armed.set(true)
          conn.submit(Connection.watch(key, rest*), complete)
        }
      }

    def run[A](command: Command[A]): CIO[A] =
      if (isReleased)
        CIO.fail(TxSupport.scopeReleasedError)
      else if (command.isBlocking)
        CIO.fail(new IllegalArgumentException("a Transaction cannot run blocking commands; run them individually on the client"))
      else
        CIO.async[A](complete => submitting(complete)(conn.submit(command, complete)))

    def discard: CIO[Unit] =
      CIO.async[Unit] { complete =>
        submitting(complete) {
          armed.set(false)
          conn.submit(Connection.unwatch, complete)
        }
      }

    def exec[Out, R](p: Pipeline[Out, R]): CIO[Option[Out]] =
      runExec(p).flatMap {
        case None          => CIO.value(None)
        case Some(results) => TxSupport.collapseStrict(results, p.toOut).map(Some(_))
      }

    def execAttempt[Out, R](p: Pipeline[Out, R]): CIO[Option[R]] =
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
            submitting(complete)(conn.submitRaw(Connection.multi +: p.commands :+ Connection.exec, complete))
          }
          .flatMap { frames =>
            armed.set(false) // EXEC clears WATCH/MULTI state server-side whether it committed or aborted
            TxSupport.interpretExec(p.commands, frames)
          }
  }
}
