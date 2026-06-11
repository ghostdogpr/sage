package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

/**
  * One ACL user's rules, as parsed from `ACL GETUSER`. `commands`/`keys`/`channels` are the server's rule strings (`+@all`, `~*`, `&*`);
  * the flag and selector vocabulary evolves, so they decode leniently rather than into a closed model.
  */
final case class AclUser(
  flags: Vector[String],
  passwords: Vector[String],
  commands: String,
  keys: String,
  channels: String,
  selectors: Vector[Map[String, String]]
)

/**
  * One `ACL LOG` entry: a denied access attempt the server recorded.
  */
final case class AclLogEntry(
  count: Long,
  reason: String,
  context: String,
  obj: String,
  username: String,
  ageSeconds: Double,
  clientInfo: String,
  entryId: Long
)

/**
  * Access-control introspection. Reads only (`WHOAMI`, `LIST`, `USERS`, `CAT`, `GETUSER`, `LOG`). The mutations — user provisioning
  * (`SETUSER`/`DELUSER`/`GENPASS`/`DRYRUN`) and the rule-file/ledger operations (`LOAD`/`SAVE`/`LOG RESET`) — are deliberately not exposed,
  * being deployment-time operator tooling rather than an app-client concern.
  */
private[sage] object Acl {

  private val WhoAmI   = Bytes.utf8("WHOAMI")
  private val ListWord = Bytes.utf8("LIST")
  private val Users    = Bytes.utf8("USERS")
  private val Cat      = Bytes.utf8("CAT")
  private val GetUser  = Bytes.utf8("GETUSER")
  private val Log      = Bytes.utf8("LOG")

  val aclWhoAmI: Command[String]        = Command("ACL", Command.NoKeys, Vector(WhoAmI), Decode.utf8String)
  val aclList: Command[Vector[String]]  = Command("ACL", Command.NoKeys, Vector(ListWord), Decode.vector(Decode.utf8String))
  val aclUsers: Command[Vector[String]] = Command("ACL", Command.NoKeys, Vector(Users), Decode.vector(Decode.utf8String))

  def aclCat(category: Option[String] = None): Command[Vector[String]] =
    Command("ACL", Command.NoKeys, Cat +: category.map(Bytes.utf8).toVector, Decode.vector(Decode.utf8String))

  def aclGetUser(username: String): Command[Option[AclUser]] =
    Command("ACL", Command.NoKeys, Vector(GetUser, Bytes.utf8(username)), decodeUser)

  def aclLog(count: Option[Long] = None): Command[Vector[AclLogEntry]] =
    Command("ACL", Command.NoKeys, Log +: count.map(n => Bytes.utf8(n.toString)).toVector, Decode.vector(decodeLogEntry))

  private val decodeUser: Frame => Either[DecodeError, Option[AclUser]] = {
    case Frame.Null => Right(None)
    case frame      =>
      Decode.fieldMap(frame).map { fields =>
        Some(
          AclUser(
            flags = strings(fields.get("flags")),
            passwords = strings(fields.get("passwords")),
            commands = string(fields, "commands"),
            keys = string(fields, "keys"),
            channels = string(fields, "channels"),
            selectors = fields.get("selectors") match {
              case Some(Frame.Array(rows)) => rows.flatMap(selectorOf)
              case _                       => Vector.empty
            }
          )
        )
      }
  }

  private def selectorOf(frame: Frame): Option[Map[String, String]] =
    Decode.fieldMap(frame).toOption.map(_.collect { case (k, Frame.BulkString(v)) => k -> v.asUtf8String })

  private def decodeLogEntry(frame: Frame): Either[DecodeError, AclLogEntry] =
    Decode.fieldMap(frame).map { fields =>
      AclLogEntry(
        count = long(fields, "count"),
        reason = string(fields, "reason"),
        context = string(fields, "context"),
        obj = string(fields, "object"),
        username = string(fields, "username"),
        ageSeconds = fields.get("age-seconds").flatMap(asDouble).getOrElse(0.0),
        clientInfo = string(fields, "client-info"),
        entryId = long(fields, "entry-id")
      )
    }

  private def string(fields: Map[String, Frame], key: String): String =
    fields
      .get(key)
      .flatMap {
        case Frame.BulkString(b)   => Some(b.asUtf8String)
        case Frame.SimpleString(s) => Some(s)
        case _                     => None
      }
      .getOrElse("")

  private def long(fields: Map[String, Frame], key: String): Long =
    fields.get(key).collect { case Frame.Integer(n) => n }.getOrElse(0L)

  private def strings(frame: Option[Frame]): Vector[String] =
    frame match {
      case Some(Frame.Array(elements)) => elements.collect { case Frame.BulkString(b) => b.asUtf8String }
      case Some(Frame.Set(elements))   => elements.collect { case Frame.BulkString(b) => b.asUtf8String }
      case _                           => Vector.empty
    }

  private def asDouble(frame: Frame): Option[Double] =
    frame match {
      case Frame.Double(v)     => Some(v)
      case Frame.Integer(v)    => Some(v.toDouble)
      case Frame.BulkString(b) => b.asUtf8String.toDoubleOption
      case _                   => None
    }
}
