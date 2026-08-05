package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.protocol.Frame

private[sage] object Connection {

  val multi: Command[Unit] = Command("MULTI", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  // the reply (an array of per-command results, or a null array on WATCH abort) is interpreted by the runtime, not this passthrough decoder
  val exec: Command[Frame] = Command("EXEC", keyIndices = Command.NoKeys, args = Vector.empty, decode = frame => Right(frame))

  val unwatch: Command[Unit] = Command("UNWATCH", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  // prefixes a single command redirected by `ASK`, telling the target node to serve the key it is importing for this one command
  val asking: Command[Unit] = Command("ASKING", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  // Puts a cluster replica connection into read-only mode so it serves reads for slots its master owns instead of answering MOVED. Issued
  // once at connection setup on replica connections only (re-issued on reconnect); a master connection never sends it, staying read-write.
  val readonly: Command[Unit] = Command("READONLY", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  // Enable RESP3 client-side caching in opt-in mode. The server tracks a read only when CLIENT CACHING YES immediately precedes it and sends
  // invalidation messages on this connection. Run this once during connection setup.
  val clientTrackingOnOptin: Command[Unit] =
    Command("CLIENT", keyIndices = Command.NoKeys, args = Vector("TRACKING", "ON", "OPTIN").map(Bytes.utf8), decode = Decode.ok)

  // enable tracking for the next command. Send this immediately before a cached read and discard its reply.
  val clientCachingYes: Command[Unit] =
    Command("CLIENT", keyIndices = Command.NoKeys, args = Vector("CACHING", "YES").map(Bytes.utf8), decode = Decode.ok)

  def isClientTracking(command: Command[?]): Boolean =
    command.name == "CLIENT" && command.args.headOption.exists(_.asUtf8String == "TRACKING")

  def watch[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Unit] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command("WATCH", keyIndices = Vector.range(0, keys.length), args = keys, decode = Decode.ok)
  }

  def ping(message: Option[String] = None): Command[String] =
    Command(
      "PING",
      keyIndices = Command.NoKeys,
      args = message.map(Bytes.utf8).toVector,
      decode = {
        case Frame.SimpleString(value) => Right(value)
        case Frame.BulkString(value)   => Right(value.asUtf8String)
        case other                     => Left(DecodeError("simple or bulk string", Frame.describe(other)))
      }
    )

  /**
    * The protocol handshake. Unknown reply entries are ignored for forward compatibility.
    */
  def hello(auth: Option[(String, String)] = None): Command[HelloReply] =
    Command(
      "HELLO",
      keyIndices = Command.NoKeys,
      args = Bytes.utf8("3") +: auth.toVector.flatMap { case (username, password) => Vector("AUTH", username, password).map(Bytes.utf8) },
      decode = HelloReply.decode
    )

  // These commands configure connection state during the HELLO setup and run again after reconnecting. They are not exposed as ordinary
  // operations because concurrent users of a shared connection would all observe the changed state.

  def select(database: Int): Command[Unit] =
    Command("SELECT", Command.NoKeys, Vector(Bytes.utf8(database.toString)), Decode.ok)

  def clientSetName(name: String): Command[Unit] =
    Command("CLIENT", Command.NoKeys, Vector("SETNAME", name).map(Bytes.utf8), Decode.ok)

  // announces the client library to CLIENT INFO/CLIENT LIST (Redis 7.2+/Valkey); one call per attribute (LIB-NAME, LIB-VER)
  def clientSetInfo(attribute: String, value: String): Command[Unit] =
    Command("CLIENT", Command.NoKeys, Vector("SETINFO", attribute, value).map(Bytes.utf8), Decode.ok)

  def echo(message: String): Command[String] =
    Command("ECHO", Command.NoKeys, Vector(Bytes.utf8(message)), Decode.utf8String)

  val clientId: Command[Long]        = Command("CLIENT", Command.NoKeys, Vector(Bytes.utf8("ID")), Decode.long)
  val clientGetName: Command[String] =
    Command(
      "CLIENT",
      Command.NoKeys,
      Vector(Bytes.utf8("GETNAME")),
      {
        case Frame.Null              => Right("")
        case Frame.BulkString(bytes) => Right(bytes.asUtf8String)
        case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
      }
    )
  val clientInfo: Command[String]    = Command("CLIENT", Command.NoKeys, Vector(Bytes.utf8("INFO")), Decode.text)
  val clientList: Command[String]    = Command("CLIENT", Command.NoKeys, Vector(Bytes.utf8("LIST")), Decode.text)
  val clientGetRedir: Command[Long]  = Command("CLIENT", Command.NoKeys, Vector(Bytes.utf8("GETREDIR")), Decode.long)
}
