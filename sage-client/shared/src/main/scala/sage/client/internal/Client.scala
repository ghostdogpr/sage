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
