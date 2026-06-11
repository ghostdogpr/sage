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

  // enables RESP3 server-assisted client-side caching in opt-in mode on this connection: no key is tracked until a CLIENT CACHING YES
  // precedes the read, and invalidation pushes arrive on this same connection. Run once per connection at bootstrap.
  val clientTrackingOnOptin: Command[Unit] =
    Command("CLIENT", keyIndices = Command.NoKeys, args = Vector("TRACKING", "ON", "OPTIN").map(Bytes.utf8), decode = Decode.ok)

  // opts the single command that immediately follows it on the wire into tracking; written adjacently before a cached read, reply discarded
  val clientCachingYes: Command[Unit] =
    Command("CLIENT", keyIndices = Command.NoKeys, args = Vector("CACHING", "YES").map(Bytes.utf8), decode = Decode.ok)

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
