package sage.client.internal

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.commands.*

final private[client] class LockCommands[K](leaseDuration: FiniteDuration, namespace: String)(using codec: KeyCodec[K]) {
  import LockCommands.Operation

  private val namespaced = SingleKeyScript.namespaced(namespace)

  def key(value: K): Bytes = namespaced(codec.encode(value))

  def command(key: Bytes, token: String, operation: Operation, cached: Boolean): Command[Boolean] =
    Command(
      LockCommands.compiled.verb(cached),
      SingleKeyScript.KeyIndices,
      Vector(
        LockCommands.compiled.reference(cached),
        SingleKeyScript.NumKeys,
        key,
        Bytes.utf8(token),
        Bytes.utf8(operation.wireName),
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
  enum Operation(val wireName: String) {
    case Acquire extends Operation("acquire")
    case Renew   extends Operation("renew")
    case Release extends Operation("release")
  }

  val script: String =
    """local token = ARGV[1]
      |local operation = ARGV[2]
      |if operation == 'acquire' then
      |  if redis.call('SET', KEYS[1], token, 'NX', 'PX', ARGV[3]) then return 1 end
      |  if redis.call('GET', KEYS[1]) == token then return redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
      |  return 0
      |end
      |if operation ~= 'renew' and operation ~= 'release' then
      |  return redis.error_reply('SAGE invalid lock operation')
      |end
      |if redis.call('GET', KEYS[1]) ~= token then return 0 end
      |if operation == 'renew' then return redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
      |return redis.call('DEL', KEYS[1])
      |""".stripMargin

  val compiled = SingleKeyScript(script)
}
