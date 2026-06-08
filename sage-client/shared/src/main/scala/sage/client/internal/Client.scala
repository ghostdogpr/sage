package sage.client.internal

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.SageException.{ServerError, UnsupportedServer}
import sage.client.SageConfig
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * The user-facing handle owning all connections to one server. Per-command methods are concrete sugar delegating to [[run]], so anything
  * implementing `run` — a fake, or a backend adapter lowering `F` to its native effect — gets the whole command surface.
  */
trait Client[F[_]] {

  def run[A](command: Command[A]): F[A]

  def close: F[Unit]

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

  final def copy[K: KeyCodec](source: K, destination: K, replace: Boolean = false): F[Boolean] =
    run(Keys.copy(source, destination, replace))

  final def del[K: KeyCodec](first: K, rest: K*): F[Long] = run(Keys.del(first, rest*))

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
}

object Client {

  def connect(config: SageConfig): CIO[Client[CIO]] =
    connectWith((onFrame, onClosed) => SocketTransport.connect(config.host, config.port, config.connectTimeout, onFrame, onClosed))

  private[client] def connectWith(factory: Multiplexer.TransportFactory): CIO[Client[CIO]] =
    CIO.blocking(new Live(new Multiplexer(factory))).flatMap { client =>
      client
        .run(Connection.hello())
        .fold(
          _ => CIO.value(client),
          error => client.close.flatMap(_ => CIO.fail(translateHandshake(error)))
        )
    }

  // pre-6.0 Redis answers HELLO with an unknown-command error; newer servers reject an unsupported protocol version with NOPROTO
  private def translateHandshake(error: Throwable): Throwable =
    error match {
      case ServerError(message) if message.startsWith("NOPROTO") || message.toLowerCase.contains("unknown command") =>
        UnsupportedServer(s"sage requires RESP3 (Redis 6.0+ or any Valkey); server rejected HELLO 3: $message")
      case other                                                                                                    => other
    }

  final private class Live(multiplexer: Multiplexer) extends Client[CIO] {

    def run[A](command: Command[A]): CIO[A] = CIO.async(callback => multiplexer.submit(command, callback))

    def close: CIO[Unit] = CIO.blocking(multiplexer.close())
  }
}
