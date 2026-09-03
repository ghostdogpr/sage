package sage.client.internal

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.commands.*

final private[client] class LockCommands[K](leaseDuration: FiniteDuration, namespace: String)(using codec: KeyCodec[K]) {
  private val prefix = {
    val ns = Bytes.utf8(namespace)
    Bytes.concat(Vector(Bytes.utf8(s"${ns.length}:"), ns, Bytes.utf8(":")))
  }

  def key(value: K): Bytes = Bytes.concat(Vector(prefix, codec.encode(value)))

  def command(key: Bytes, token: String, operation: String, cached: Boolean): Command[Boolean] =
    Command(
      if (cached) "EVALSHA" else "EVAL",
      Vector(2),
      Vector(
        if (cached) LockCommands.digest else LockCommands.body,
        Bytes.utf8("1"),
        key,
        Bytes.utf8(token),
        Bytes.utf8(operation),
        Bytes.utf8(leaseDuration.toMillis.toString)
      ),
      _.asLong.flatMap {
        case 0 => Right(false)
        case 1 => Right(true)
        case n => Left(DecodeError("lock result 0 or 1", n.toString))
      }
    )
}

private[client] object LockCommands {
  val script: String =
    """local token = ARGV[1]
      |local operation = ARGV[2]
      |if operation == 'acquire' then
      |  if redis.call('SET', KEYS[1], token, 'NX', 'PX', ARGV[3]) then return 1 end
      |  return 0
      |end
      |if operation ~= 'renew' and operation ~= 'release' then
      |  return redis.error_reply('SAGE invalid lock operation')
      |end
      |if redis.call('GET', KEYS[1]) ~= token then return 0 end
      |if operation == 'renew' then return redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
      |return redis.call('DEL', KEYS[1])
      |""".stripMargin

  val body: Bytes   = Bytes.utf8(script)
  val digest: Bytes = Bytes.utf8(
    java.security.MessageDigest.getInstance("SHA-1").digest(body.toArray).iterator.map(b => f"${b & 0xff}%02x").mkString
  )
}
