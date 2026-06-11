package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

/**
  * The shared flush mode for `SCRIPT FLUSH`, `FUNCTION FLUSH`, `FLUSHALL`, `FLUSHDB`. Omitting it defers to the server's
  * `lazyfree-lazy-user-flush` default, so builders carry it as an `Option`.
  */
enum FlushMode {
  case Async, Sync
}

object FlushMode {
  private[commands] def args(mode: Option[FlushMode]): Vector[Bytes] = mode.map(m => Bytes.utf8(m.toString.toUpperCase)).toVector
}

/**
  * The reply of `ROLE`: which replication role the contacted node plays, with the per-role detail.
  */
enum Role {
  case Master(replicationOffset: Long, replicas: Vector[ReplicaNode])
  case Replica(masterHost: String, masterPort: Int, state: String, replicationOffset: Long)
  case Sentinel(masterNames: Vector[String])
}

final case class ReplicaNode(host: String, port: Int, replicationOffset: Long)

/**
  * One `SLOWLOG GET` entry. `clientAddr`/`clientName` are absent on servers older than 4.0, so they decode to empty strings rather than
  * failing.
  */
final case class SlowLogEntry(id: Long, timestamp: Instant, duration: FiniteDuration, command: Vector[String], clientAddr: String, clientName: String)

final case class LatencyEntry(event: String, timestamp: Instant, latest: FiniteDuration, max: FiniteDuration)

/**
  * The Valkey command log's three tracked categories. `Slow` logs by execution time; `LargeRequest`/`LargeReply` log by payload size.
  */
enum CommandLogType {
  case Slow, LargeRequest, LargeReply
}

object CommandLogType {

  private[commands] def wire(tpe: CommandLogType): Bytes =
    Bytes.utf8(tpe match {
      case Slow         => "slow"
      case LargeRequest => "large-request"
      case LargeReply   => "large-reply"
    })
}

/**
  * One `COMMANDLOG GET` entry. `metric` is microseconds for [[CommandLogType.Slow]] and bytes for the large-request/large-reply logs — the
  * caller already chose the type, so the unit is known. `clientAddr`/`clientName` may be empty on older entries.
  */
final case class CommandLogEntry(
  id: Long,
  timestamp: Instant,
  metric: Long,
  command: Vector[String],
  clientAddr: String,
  clientName: String
)

final case class CommandHistogram(calls: Long, histogramUsec: Map[Long, Long])

/**
  * `COMMAND INFO`'s stable classic fields; deeper key-specs, tips, and subcommands are intentionally not decoded.
  */
final case class CommandInfo(name: String, arity: Long, flags: Set[String], firstKey: Int, lastKey: Int, step: Int, aclCategories: Set[String])

/**
  * `COMMAND LIST FILTERBY` selector.
  */
enum CommandFilterBy {
  case Module(name: String)
  case AclCat(category: String)
  case Pattern(glob: String)
}

/**
  * Server administration: configuration, introspection, persistence, replication waits, and a read-only slice of cluster introspection.
  * Most are keyless and route to one arbitrary master in a cluster. Text-format replies (`INFO`, `CLUSTER INFO`/`NODES`) are returned as
  * raw `String`; structured replies decode to typed ADTs.
  */
private[sage] object Server {

  private val Get             = Bytes.utf8("GET")
  private val Set             = Bytes.utf8("SET")
  private val Usage           = Bytes.utf8("USAGE")
  private val Samples         = Bytes.utf8("SAMPLES")
  private val Purge           = Bytes.utf8("PURGE")
  private val SlowLen         = Bytes.utf8("LEN")
  private val History         = Bytes.utf8("HISTORY")
  private val Latest          = Bytes.utf8("LATEST")
  private val Reset           = Bytes.utf8("RESET")
  private val Histogram       = Bytes.utf8("HISTOGRAM")
  private val Count           = Bytes.utf8("COUNT")
  private val ListCmd         = Bytes.utf8("LIST")
  private val GetKeys         = Bytes.utf8("GETKEYS")
  private val GetKeysAndFlags = Bytes.utf8("GETKEYSANDFLAGS")
  private val Info            = Bytes.utf8("INFO")
  private val FilterBy        = Bytes.utf8("FILTERBY")
  private val Module          = Bytes.utf8("MODULE")
  private val AclCat          = Bytes.utf8("ACLCAT")
  private val PatternWord     = Bytes.utf8("PATTERN")
  private val ClInfo          = Bytes.utf8("INFO")
  private val ClNodes         = Bytes.utf8("NODES")
  private val ClMyId          = Bytes.utf8("MYID")
  private val ClKeySlot       = Bytes.utf8("KEYSLOT")
  private val ClCountKeys     = Bytes.utf8("COUNTKEYSINSLOT")

  def configGet(parameter: String, rest: String*): Command[Map[String, String]] =
    Command("CONFIG", Command.NoKeys, Get +: (parameter +: rest).iterator.map(Bytes.utf8).toVector, decodeStringMap)

  def configSet(setting: (String, String), rest: (String, String)*): Command[Unit] =
    Command("CONFIG", Command.NoKeys, Set +: (setting +: rest).flatMap { case (k, v) => Vector(Bytes.utf8(k), Bytes.utf8(v)) }.toVector, Decode.ok)

  def info(sections: String*): Command[String] =
    Command("INFO", Command.NoKeys, sections.iterator.map(Bytes.utf8).toVector, Decode.text)

  val dbSize: Command[Long] = Command("DBSIZE", Command.NoKeys, Vector.empty, Decode.long)

  val time: Command[Instant] =
    Command(
      "TIME",
      Command.NoKeys,
      Vector.empty,
      {
        case Frame.Array(Vector(Frame.BulkString(sec), Frame.BulkString(micros))) =>
          for {
            s <- sec.asUtf8String.toLongOption.toRight(DecodeError("epoch seconds", sec.asUtf8String))
            u <- micros.asUtf8String.toLongOption.toRight(DecodeError("microseconds", micros.asUtf8String))
          } yield Instant.ofEpochSecond(s, u * 1000L)
        case other                                                                => Left(DecodeError("TIME [seconds, microseconds]", Frame.describe(other)))
      }
    )

  private val decodeRole: Frame => Either[DecodeError, Role] = {
    case Frame.Array(Frame.BulkString(kind) +: rest) =>
      kind.asUtf8String match {
        case "master"   =>
          rest match {
            case Frame.Integer(offset) +: replicasFrame +: _ => decodeReplicas(replicasFrame).map(Role.Master(offset, _))
            case other                                       => Left(DecodeError("master role [offset, replicas]", other.map(Frame.describe).mkString(", ")))
          }
        case "slave"    =>
          rest match {
            case Vector(Frame.BulkString(host), Frame.Integer(port), Frame.BulkString(state), Frame.Integer(offset)) =>
              Right(Role.Replica(host.asUtf8String, port.toInt, state.asUtf8String, offset))
            case other                                                                                               =>
              Left(DecodeError("replica role [host, port, state, offset]", other.map(Frame.describe).mkString(", ")))
          }
        case "sentinel" =>
          rest.headOption match {
            case Some(masters) => Decode.vector(Decode.utf8String)(masters).map(Role.Sentinel(_))
            case None          => Left(DecodeError("sentinel role [masterNames]", "empty"))
          }
        case other      => Left(DecodeError("role master|slave|sentinel", other))
      }
    case other                                       => Left(DecodeError("ROLE array", Frame.describe(other)))
  }

  val role: Command[Role] = Command("ROLE", Command.NoKeys, Vector.empty, decodeRole)

  // each master holds its own keyspace shard, so flushing the logical database means reaching every master, not one arbitrary node
  def flushAll(mode: Option[FlushMode] = None): Command[Unit] =
    Command("FLUSHALL", Command.NoKeys, FlushMode.args(mode), Decode.ok, allMasters = true)
  def flushDb(mode: Option[FlushMode] = None): Command[Unit]  =
    Command("FLUSHDB", Command.NoKeys, FlushMode.args(mode), Decode.ok, allMasters = true)

  def waitReplicas(numReplicas: Long, timeout: FiniteDuration): Command[Long] =
    Command("WAIT", Command.NoKeys, Vector(Bytes.utf8(numReplicas.toString), Bytes.utf8(timeout.toMillis.toString)), Decode.long, Execution.Blocking)

  def waitAof(numLocal: Long, numReplicas: Long, timeout: FiniteDuration): Command[(Long, Long)] =
    Command(
      "WAITAOF",
      Command.NoKeys,
      Vector(numLocal, numReplicas, timeout.toMillis).map(n => Bytes.utf8(n.toString)),
      {
        case Frame.Array(Vector(Frame.Integer(local), Frame.Integer(replicas))) => Right((local, replicas))
        case other                                                              => Left(DecodeError("WAITAOF [numlocal, numreplicas]", Frame.describe(other)))
      },
      Execution.Blocking
    )

  def memoryUsage[K](key: K, samples: Option[Long] = None)(using keyCodec: sage.codec.KeyCodec[K]): Command[Option[Long]] =
    Command(
      "MEMORY",
      Vector(1),
      Vector(Usage, keyCodec.encode(key)) ++ samples.toVector.flatMap(n => Vector(Samples, Bytes.utf8(n.toString))),
      Decode.optionalLong
    )

  val memoryPurge: Command[Unit] = Command("MEMORY", Command.NoKeys, Vector(Purge), Decode.ok)

  def slowLogGet(count: Option[Long] = None): Command[Vector[SlowLogEntry]] =
    Command("SLOWLOG", Command.NoKeys, Get +: count.map(n => Bytes.utf8(n.toString)).toVector, Decode.vector(decodeSlowLog))

  val slowLogLen: Command[Long]   = Command("SLOWLOG", Command.NoKeys, Vector(SlowLen), Decode.long)
  val slowLogReset: Command[Unit] = Command("SLOWLOG", Command.NoKeys, Vector(Reset), Decode.ok)

  // Valkey command-log observability; `count` of -1 returns every entry of the type
  def commandLogGet(count: Long, logType: CommandLogType): Command[Vector[CommandLogEntry]] =
    Command("COMMANDLOG", Command.NoKeys, Vector(Get, Bytes.utf8(count.toString), CommandLogType.wire(logType)), Decode.vector(decodeCommandLog))

  def commandLogLen(logType: CommandLogType): Command[Long] =
    Command("COMMANDLOG", Command.NoKeys, Vector(SlowLen, CommandLogType.wire(logType)), Decode.long)

  def commandLogReset(logType: CommandLogType): Command[Unit] =
    Command("COMMANDLOG", Command.NoKeys, Vector(Reset, CommandLogType.wire(logType)), Decode.ok)

  def latencyHistory(event: String): Command[Vector[(Instant, FiniteDuration)]] =
    Command("LATENCY", Command.NoKeys, Vector(History, Bytes.utf8(event)), Decode.vector(decodeLatencyHistory))

  val latencyLatest: Command[Vector[LatencyEntry]] = Command("LATENCY", Command.NoKeys, Vector(Latest), Decode.vector(decodeLatencyLatest))

  def latencyReset(events: String*): Command[Long] =
    Command("LATENCY", Command.NoKeys, Reset +: events.iterator.map(Bytes.utf8).toVector, Decode.long)

  def latencyHistogram(commands: String*): Command[Map[String, CommandHistogram]] =
    Command("LATENCY", Command.NoKeys, Histogram +: commands.iterator.map(Bytes.utf8).toVector, decodeHistograms)

  val commandCount: Command[Long] = Command("COMMAND", Command.NoKeys, Vector(Count), Decode.long)

  def commandList(filterBy: Option[CommandFilterBy] = None): Command[Vector[String]] =
    Command("COMMAND", Command.NoKeys, ListCmd +: filterByArgs(filterBy), Decode.vector(Decode.utf8String))

  def commandGetKeys(command: String, args: String*): Command[Vector[String]] =
    Command("COMMAND", Command.NoKeys, GetKeys +: Bytes.utf8(command) +: args.iterator.map(Bytes.utf8).toVector, Decode.vector(Decode.utf8String))

  def commandGetKeysAndFlags(command: String, args: String*): Command[Vector[(String, Set[String])]] =
    Command(
      "COMMAND",
      Command.NoKeys,
      GetKeysAndFlags +: Bytes.utf8(command) +: args.iterator.map(Bytes.utf8).toVector,
      Decode.vector(decodeKeyAndFlags)
    )

  def commandInfo(commands: String*): Command[Vector[CommandInfo]] =
    Command("COMMAND", Command.NoKeys, Info +: commands.iterator.map(Bytes.utf8).toVector, decodeCommandInfos)

  // --- cluster introspection (read-only; operator/mutation commands are deliberately not exposed) ----------------------------------------

  val clusterInfo: Command[String]  = Command("CLUSTER", Command.NoKeys, Vector(ClInfo), Decode.text)
  val clusterNodes: Command[String] = Command("CLUSTER", Command.NoKeys, Vector(ClNodes), Decode.text)
  val clusterMyId: Command[String]  = Command("CLUSTER", Command.NoKeys, Vector(ClMyId), Decode.text)

  def clusterKeySlot(key: String): Command[Long] =
    Command("CLUSTER", Command.NoKeys, Vector(ClKeySlot, Bytes.utf8(key)), Decode.long)

  def clusterCountKeysInSlot(slot: Int): Command[Long] =
    Command("CLUSTER", Command.NoKeys, Vector(ClCountKeys, Bytes.utf8(slot.toString)), Decode.long)

  // --- decoders --------------------------------------------------------------------------------------------------------------------------

  private val decodeStringMap: Frame => Either[DecodeError, Map[String, String]] =
    frame =>
      Decode.fieldMap(frame).flatMap { fields =>
        fields.foldLeft[Either[DecodeError, Map[String, String]]](Right(Map.empty)) { case (acc, (name, valueFrame)) =>
          for {
            map   <- acc
            value <- Decode.text(valueFrame)
          } yield map + (name -> value)
        }
      }

  private def filterByArgs(filterBy: Option[CommandFilterBy]): Vector[Bytes] =
    filterBy match {
      case None                                   => Vector.empty
      case Some(CommandFilterBy.Module(name))     => Vector(FilterBy, Module, Bytes.utf8(name))
      case Some(CommandFilterBy.AclCat(category)) => Vector(FilterBy, AclCat, Bytes.utf8(category))
      case Some(CommandFilterBy.Pattern(glob))    => Vector(FilterBy, PatternWord, Bytes.utf8(glob))
    }

  private def decodeReplicas(frame: Frame): Either[DecodeError, Vector[ReplicaNode]] =
    Decode.vector {
      case Frame.Array(Vector(Frame.BulkString(host), Frame.BulkString(port), Frame.BulkString(offset))) =>
        for {
          p <- port.asUtf8String.toIntOption.toRight(DecodeError("replica port", port.asUtf8String))
          o <- offset.asUtf8String.toLongOption.toRight(DecodeError("replica offset", offset.asUtf8String))
        } yield ReplicaNode(host.asUtf8String, p, o)
      case other                                                                                         => Left(DecodeError("replica [host, port, offset]", Frame.describe(other)))
    }(frame)

  // SLOWLOG GET and COMMANDLOG GET share this trailing [clientAddr, clientName] shape, absent on servers older than 4.0
  private def clientFields(tail: Vector[Frame]): (String, String) = {
    val addr = tail.headOption.collect { case Frame.BulkString(b) => b.asUtf8String }.getOrElse("")
    val name = tail.drop(1).headOption.collect { case Frame.BulkString(b) => b.asUtf8String }.getOrElse("")
    (addr, name)
  }

  private def decodeSlowLog(frame: Frame): Either[DecodeError, SlowLogEntry] =
    frame match {
      case Frame.Array(Frame.Integer(id) +: Frame.Integer(ts) +: Frame.Integer(micros) +: argsFrame +: tail) =>
        Decode.vector(Decode.utf8String)(argsFrame).map { command =>
          val (addr, name) = clientFields(tail)
          SlowLogEntry(id, Instant.ofEpochSecond(ts), micros.micros, command, addr, name)
        }
      case other                                                                                             => Left(DecodeError("slowlog entry", Frame.describe(other)))
    }

  private def decodeCommandLog(frame: Frame): Either[DecodeError, CommandLogEntry] =
    frame match {
      case Frame.Array(Frame.Integer(id) +: Frame.Integer(ts) +: Frame.Integer(metric) +: argsFrame +: tail) =>
        Decode.vector(Decode.utf8String)(argsFrame).map { command =>
          val (addr, name) = clientFields(tail)
          CommandLogEntry(id, Instant.ofEpochSecond(ts), metric, command, addr, name)
        }
      case other                                                                                             => Left(DecodeError("commandlog entry", Frame.describe(other)))
    }

  private def decodeLatencyLatest(frame: Frame): Either[DecodeError, LatencyEntry] =
    frame match {
      case Frame.Array(Vector(Frame.BulkString(event), Frame.Integer(ts), Frame.Integer(latest), Frame.Integer(max))) =>
        Right(LatencyEntry(event.asUtf8String, Instant.ofEpochSecond(ts), latest.millis, max.millis))
      case other                                                                                                      =>
        Left(DecodeError("latency latest [event, ts, latest, max]", Frame.describe(other)))
    }

  private def decodeLatencyHistory(frame: Frame): Either[DecodeError, (Instant, FiniteDuration)] =
    frame match {
      case Frame.Array(Vector(Frame.Integer(ts), Frame.Integer(latency))) => Right((Instant.ofEpochSecond(ts), latency.millis))
      case other                                                          => Left(DecodeError("latency history [ts, latency]", Frame.describe(other)))
    }

  private val decodeHistograms: Frame => Either[DecodeError, Map[String, CommandHistogram]] = {
    case Frame.Map(entries) =>
      entries.foldLeft[Either[DecodeError, Map[String, CommandHistogram]]](Right(Map.empty)) { case (acc, (nameFrame, statsFrame)) =>
        for {
          map  <- acc
          name <- Decode.utf8String(nameFrame)
          hist <- decodeHistogram(statsFrame)
        } yield map + (name -> hist)
      }
    case other              => Left(DecodeError("latency histogram map", Frame.describe(other)))
  }

  private def decodeHistogram(frame: Frame): Either[DecodeError, CommandHistogram] =
    frame match {
      case Frame.Map(entries) =>
        val fields  = entries.collect { case (Frame.BulkString(k), v) => k.asUtf8String -> v }.toMap
        val calls   = fields.get("calls").collect { case Frame.Integer(n) => n }.getOrElse(0L)
        val buckets = fields.get("histogram_usec") match {
          case Some(Frame.Map(bs)) =>
            bs.collect { case (Frame.Integer(bucket), Frame.Integer(count)) => bucket -> count }.toMap
          case _                   => Map.empty[Long, Long]
        }
        Right(CommandHistogram(calls, buckets))
      case other              => Left(DecodeError("command histogram map", Frame.describe(other)))
    }

  // a server may frame a flag list as a RESP3 Set or an Array
  private def stringSeq(frame: Frame): Either[DecodeError, Vector[String]] =
    frame match {
      case Frame.Set(elements) => Decode.vector(Decode.text)(Frame.Array(elements))
      case other               => Decode.vector(Decode.text)(other)
    }

  private def decodeKeyAndFlags(frame: Frame): Either[DecodeError, (String, Set[String])] =
    frame match {
      case Frame.Array(Vector(Frame.BulkString(key), flagsFrame)) =>
        stringSeq(flagsFrame).map(flags => key.asUtf8String -> flags.toSet)
      case other                                                  => Left(DecodeError("[key, [flags]]", Frame.describe(other)))
    }

  // COMMAND INFO yields one element per requested name; an unknown name is a null element, dropped here
  private val decodeCommandInfos: Frame => Either[DecodeError, Vector[CommandInfo]] = {
    case Frame.Array(elements) =>
      elements.foldLeft[Either[DecodeError, Vector[CommandInfo]]](Right(Vector.empty)) { (acc, element) =>
        acc.flatMap { infos =>
          element match {
            case Frame.Null => Right(infos)
            case other      => decodeCommandInfo(other).map(infos :+ _)
          }
        }
      }
    case other                 => Left(DecodeError("COMMAND INFO array", Frame.describe(other)))
  }

  private def decodeCommandInfo(frame: Frame): Either[DecodeError, CommandInfo] =
    frame match {
      case Frame.Array(
            Frame.BulkString(name) +: Frame.Integer(arity) +: flagsFrame +: Frame.Integer(firstKey) +: Frame.Integer(lastKey) +: Frame.Integer(
              step
            ) +: tail
          ) =>
        for {
          flags <- stringSeq(flagsFrame)
          acl   <- tail.headOption.fold[Either[DecodeError, Vector[String]]](Right(Vector.empty))(stringSeq)
        } yield CommandInfo(name.asUtf8String, arity, flags.toSet, firstKey.toInt, lastKey.toInt, step.toInt, acl.toSet)
      case other =>
        Left(DecodeError("command info entry", Frame.describe(other)))
    }

}
