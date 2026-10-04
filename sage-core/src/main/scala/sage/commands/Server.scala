package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.Primitives
import sage.commands.Args.{Count, Get}
import sage.protocol.Frame

/**
  * The shared flush mode for `SCRIPT FLUSH`, `FUNCTION FLUSH`, `FLUSHALL`, and `FLUSHDB`. Builders accept an `Option` because omitting the mode
  * uses the server's `lazyfree-lazy-user-flush` setting.
  */
enum FlushMode {
  case Async, Sync
}

object FlushMode {
  private val word                                                   = Args.keywords(FlushMode.values)
  private[commands] def args(mode: Option[FlushMode]): Vector[Bytes] = mode.map(word).toVector
}

/**
  * The reply of `ROLE`: which replication role the contacted node plays, with the per-role detail.
  */
enum Role {
  case Master(replicationOffset: Long, replicas: Vector[ReplicaNode])
  case Replica(masterHost: String, masterPort: Int, state: String, replicationOffset: Long)
  case Sentinel(masterNames: Vector[String])

  private[sage] def isConnectedReplica: Boolean = this match {
    case Replica(_, _, "connected", _) => true
    case _                             => false
  }
}

/**
  * A replica reported by `INFO replication`/`ROLE`: its address and how far its replication stream has caught up.
  */
final case class ReplicaNode(host: String, port: Int, replicationOffset: Long)

/**
  * One `SLOWLOG GET` entry. `clientAddr`/`clientName` are absent on servers older than 4.0, so they decode to empty strings rather than
  * failing.
  */
final case class SlowLogEntry(id: Long, timestamp: Instant, duration: FiniteDuration, command: Vector[String], clientAddr: String, clientName: String)

/**
  * One `LATENCY LATEST` entry: the monitored event, when it last spiked, and its latest and all-time-max latencies.
  */
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
  * One `COMMANDLOG GET` entry. `metric` uses microseconds for [[CommandLogType.Slow]] and bytes for large-request and large-reply logs. The
  * caller selects the log type and therefore knows the unit. Older entries can have empty `clientAddr` and `clientName` values.
  */
final case class CommandLogEntry(
  id: Long,
  timestamp: Instant,
  metric: Long,
  command: Vector[String],
  clientAddr: String,
  clientName: String
)

/**
  * A per-command latency histogram from `LATENCY HISTOGRAM`: the call count and a cumulative distribution keyed by microsecond bucket.
  */
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

  private val Set             = Bytes.utf8("SET")
  private val Usage           = Bytes.utf8("USAGE")
  private val Samples         = Bytes.utf8("SAMPLES")
  private val Purge           = Bytes.utf8("PURGE")
  private val SlowLen         = Bytes.utf8("LEN")
  private val History         = Bytes.utf8("HISTORY")
  private val Latest          = Bytes.utf8("LATEST")
  private val Reset           = Bytes.utf8("RESET")
  private val Histogram       = Bytes.utf8("HISTOGRAM")
  private val ListCmd         = Bytes.utf8("LIST")
  private val GetKeys         = Bytes.utf8("GETKEYS")
  private val GetKeysAndFlags = Bytes.utf8("GETKEYSANDFLAGS")
  private val Info            = Bytes.utf8("INFO")
  private val FilterBy        = Bytes.utf8("FILTERBY")
  private val Module          = Bytes.utf8("MODULE")
  private val AclCat          = Bytes.utf8("ACLCAT")
  private val PatternWord     = Bytes.utf8("PATTERN")
  private val ClNodes         = Bytes.utf8("NODES")
  private val ClMyId          = Bytes.utf8("MYID")
  private val ClKeySlot       = Bytes.utf8("KEYSLOT")
  private val ClCountKeys     = Bytes.utf8("COUNTKEYSINSLOT")

  def configGet(parameter: String, rest: String*): Command[Map[String, String]] =
    Command("CONFIG", Command.NoKeys, Get +: (parameter +: rest).iterator.map(Bytes.utf8).toVector, Decode.fieldValues(Decode.text))

  def configSet(setting: (String, String), rest: (String, String)*): Command[Unit] =
    Command("CONFIG", Command.NoKeys, Set +: Args.pairs(setting +: rest.toVector), Decode.ok)

  def info(sections: String*): Command[String] =
    Command("INFO", Command.NoKeys, sections.iterator.map(Bytes.utf8).toVector, Decode.text)

  val dbSize: Command[Long] =
    Command(
      "DBSIZE",
      Command.NoKeys,
      Vector.empty,
      Decode.long,
      allMasters = true,
      broadcast = BroadcastReduce.Fold(Merge.sum),
      requiresClusterWideTxResult = true
    )

  val time: Command[Instant] =
    Command(
      "TIME",
      Command.NoKeys,
      Vector.empty,
      Decode.array2(Decode.decimal("epoch seconds"), Decode.decimal("microseconds"), "TIME [seconds, microseconds]")((s, u) =>
        Instant.ofEpochSecond(s, u * 1000L)
      )
    )

  private val decodeRole: Frame => Either[DecodeError, Role] = Decode.shape("ROLE array") { case Frame.Array(Frame.BulkString(kind) +: rest) =>
    kind.asUtf8String match {
      case "master"   =>
        rest match {
          case Frame.Integer(offset) +: replicasFrame +: _ => decodeReplicas(replicasFrame).map(Role.Master(offset, _))
          case other                                       => Left(DecodeError("master role [offset, replicas]", other.map(Frame.describe).mkString(", ")))
        }
      case "slave"    =>
        rest match {
          case Vector(Frame.BulkString(host), Frame.Integer(port), Frame.BulkString(state), Frame.Integer(offset)) =>
            decodePort(port).map(Role.Replica(host.asUtf8String, _, state.asUtf8String, offset))
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
  }

  val role: Command[Role] = Command("ROLE", Command.NoKeys, Vector.empty, decodeRole)

  // each master holds its own keyspace shard, so flushing the logical database means reaching every master, not one arbitrary node
  def flushAll(mode: Option[FlushMode] = None): Command[Unit] =
    Command("FLUSHALL", Command.NoKeys, FlushMode.args(mode), Decode.ok, allMasters = true)
  def flushDb(mode: Option[FlushMode] = None): Command[Unit]  =
    Command("FLUSHDB", Command.NoKeys, FlushMode.args(mode), Decode.ok, allMasters = true)

  // WAIT and WAITAOF interpret an encoded timeout of 0 as an unlimited wait. Encode Duration.Zero as 0. Round every other duration, including
  // a negative one, up to at least 1 ms so a sub-millisecond timeout remains finite.
  private def waitTimeout(timeout: FiniteDuration): Bytes =
    BlockTimeout.millisWire(if (timeout == Duration.Zero) BlockTimeout.Forever else BlockTimeout.After(timeout))

  def waitReplicas(numReplicas: Long, timeout: FiniteDuration): Command[Long] =
    Command(
      "WAIT",
      Command.NoKeys,
      Vector(Args.long(numReplicas), waitTimeout(timeout)),
      Decode.long,
      allMasters = true,
      broadcast = BroadcastReduce.Fold(Merge.min)
    )

  private val waitAofReply = Decode.shape("WAITAOF [numlocal, numreplicas]") {
    case Frame.Array(Vector(Frame.Integer(local), Frame.Integer(replicas))) => Right((local, replicas))
  }

  private val waitAofMin = Merge.typed[(Long, Long)](waitAofReply, (l, r) => Frame.Array(Vector(Frame.Integer(l), Frame.Integer(r)))) {
    case ((l1, r1), (l2, r2)) => (math.min(l1, l2), math.min(r1, r2))
  }

  def waitAof(numLocal: Long, numReplicas: Long, timeout: FiniteDuration): Command[(Long, Long)] =
    Command(
      "WAITAOF",
      Command.NoKeys,
      Vector(Args.long(numLocal), Args.long(numReplicas), waitTimeout(timeout)),
      waitAofReply,
      allMasters = true,
      broadcast = BroadcastReduce.Fold(waitAofMin)
    )

  def memoryUsage[K](key: K, samples: Option[Long] = None)(using keyCodec: sage.codec.KeyCodec[K]): Command[Option[Long]] =
    Command(
      "MEMORY",
      Vector(1),
      Vector(Usage, keyCodec.encode(key)) ++ Args.optLong(Samples, samples),
      Decode.optionalLong
    )

  val memoryPurge: Command[Unit] = Command("MEMORY", Command.NoKeys, Vector(Purge), Decode.ok, allMasters = true)

  def slowLogGet(count: Option[Long] = None): Command[Vector[SlowLogEntry]] =
    Command("SLOWLOG", Command.NoKeys, Get +: count.map(n => Args.long(n)).toVector, Decode.vector(decodeSlowLog))

  val slowLogLen: Command[Long]   = Command("SLOWLOG", Command.NoKeys, Vector(SlowLen), Decode.long)
  val slowLogReset: Command[Unit] = Command("SLOWLOG", Command.NoKeys, Vector(Reset), Decode.ok)

  // `count` of -1 returns every entry of the type
  def commandLogGet(count: Long, logType: CommandLogType): Command[Vector[CommandLogEntry]] =
    Command("COMMANDLOG", Command.NoKeys, Vector(Get, Args.long(count), CommandLogType.wire(logType)), Decode.vector(decodeCommandLog))

  def commandLogLen(logType: CommandLogType): Command[Long] =
    Command("COMMANDLOG", Command.NoKeys, Vector(SlowLen, CommandLogType.wire(logType)), Decode.long)

  def commandLogReset(logType: CommandLogType): Command[Unit] =
    Command("COMMANDLOG", Command.NoKeys, Vector(Reset, CommandLogType.wire(logType)), Decode.ok)

  def latencyHistory(event: String): Command[Vector[(Instant, FiniteDuration)]] =
    Command("LATENCY", Command.NoKeys, Vector(History, Bytes.utf8(event)), Decode.vector(decodeLatencyHistory))

  private val decodeLatencyLatest: Frame => Either[DecodeError, LatencyEntry] = Decode.shape("latency latest [event, ts, latest, max]") {
    case Frame.Array(Vector(Frame.BulkString(event), Frame.Integer(ts), Frame.Integer(latest), Frame.Integer(max))) =>
      Right(LatencyEntry(event.asUtf8String, Instant.ofEpochSecond(ts), latest.millis, max.millis))
  }

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

  val clusterInfo: Command[String]  = Command("CLUSTER", Command.NoKeys, Vector(Info), Decode.text)
  val clusterNodes: Command[String] = Command("CLUSTER", Command.NoKeys, Vector(ClNodes), Decode.text)
  val clusterMyId: Command[String]  = Command("CLUSTER", Command.NoKeys, Vector(ClMyId), Decode.text)

  def clusterKeySlot(key: String): Command[Long] =
    Command("CLUSTER", Command.NoKeys, Vector(ClKeySlot, Bytes.utf8(key)), Decode.long)

  def clusterCountKeysInSlot(slot: Int): Command[Long] =
    Command("CLUSTER", Command.NoKeys, Vector(ClCountKeys, Args.long(slot)), Decode.long)

  // --- decoders --------------------------------------------------------------------------------------------------------------------------

  private def filterByArgs(filterBy: Option[CommandFilterBy]): Vector[Bytes] =
    filterBy match {
      case None                                   => Vector.empty
      case Some(CommandFilterBy.Module(name))     => Vector(FilterBy, Module, Bytes.utf8(name))
      case Some(CommandFilterBy.AclCat(category)) => Vector(FilterBy, AclCat, Bytes.utf8(category))
      case Some(CommandFilterBy.Pattern(glob))    => Vector(FilterBy, PatternWord, Bytes.utf8(glob))
    }

  private def decodePort(value: Long): Either[DecodeError, Int] =
    if (value >= 1L && value <= 65535L) Right(value.toInt) else Left(DecodeError("port in 1..65535", value.toString))

  private val decodeReplicas: Frame => Either[DecodeError, Vector[ReplicaNode]] = Decode.vector(Decode.shape("replica [host, port, offset]") {
    case Frame.Array(Vector(Frame.BulkString(host), Frame.BulkString(port), Frame.BulkString(offset))) =>
      for {
        p <- Primitives.decodeLong("replica port in 1..65535", 1L, 65535L)(port)
        o <- Primitives.decodeLong("replica offset", Long.MinValue, Long.MaxValue)(offset)
      } yield ReplicaNode(host.asUtf8String, p.toInt, o)
  })

  // SLOWLOG GET and COMMANDLOG GET entries: [id, timestamp, metric, args, clientAddr, clientName]; the client fields are absent on servers
  // older than 4.0
  private def logEntry[A](label: String)(build: (Long, Instant, Long, Vector[String], String, String) => A): Frame => Either[DecodeError, A] =
    Decode.shape(label) { case Frame.Array(Frame.Integer(id) +: Frame.Integer(ts) +: Frame.Integer(metric) +: argsFrame +: tail) =>
      Decode.vector(Decode.utf8String)(argsFrame).map { command =>
        def client(i: Int) = tail.lift(i).collect { case Frame.BulkString(b) => b.asUtf8String }.getOrElse("")
        build(id, Instant.ofEpochSecond(ts), metric, command, client(0), client(1))
      }
    }

  private val decodeSlowLog: Frame => Either[DecodeError, SlowLogEntry] =
    logEntry("slowlog entry")((id, ts, micros, command, addr, name) => SlowLogEntry(id, ts, micros.micros, command, addr, name))

  private val decodeCommandLog: Frame => Either[DecodeError, CommandLogEntry] = logEntry("commandlog entry")(CommandLogEntry(_, _, _, _, _, _))

  private val decodeLatencyHistory: Frame => Either[DecodeError, (Instant, FiniteDuration)] = Decode.shape("latency history [ts, latency]") {
    case Frame.Array(Vector(Frame.Integer(ts), Frame.Integer(latency))) => Right((Instant.ofEpochSecond(ts), latency.millis))
  }

  private val decodeHistograms: Frame => Either[DecodeError, Map[String, CommandHistogram]] = {
    val entry = Decode.pair(Decode.utf8String, decodeHistogram).tupled
    Decode.shape("latency histogram map") { case Frame.Map(entries) => Decode.mapEntries(entries)(entry) }
  }

  private def decodeHistogram(frame: Frame): Either[DecodeError, CommandHistogram] =
    Decode.fieldMap(frame).map { fields =>
      val calls   = fields.get("calls").collect { case Frame.Integer(n) => n }.getOrElse(0L)
      val buckets = fields.get("histogram_usec") match {
        case Some(Frame.Map(bs)) =>
          bs.collect { case (Frame.Integer(bucket), Frame.Integer(count)) => bucket -> count }.toMap
        case _                   => Map.empty[Long, Long]
      }
      CommandHistogram(calls, buckets)
    }

  // a server may frame a flag list as a RESP3 Set or an Array
  private def stringSeq(frame: Frame): Either[DecodeError, Vector[String]] =
    frame match {
      case Frame.Set(elements) => Decode.vector(Decode.text)(Frame.Array(elements))
      case other               => Decode.vector(Decode.text)(other)
    }

  private val decodeKeyAndFlags: Frame => Either[DecodeError, (String, Set[String])] = Decode.shape("[key, [flags]]") {
    case Frame.Array(Vector(Frame.BulkString(key), flagsFrame)) => stringSeq(flagsFrame).map(flags => key.asUtf8String -> flags.toSet)
  }

  // COMMAND INFO yields one element per requested name; an unknown name is a null element, dropped here
  private val decodeCommandInfos: Frame => Either[DecodeError, Vector[CommandInfo]] = Decode.shape("COMMAND INFO array") {
    case Frame.Array(elements) => Decode.each(elements)(Decode.nullable(decodeCommandInfo)).map(_.flatten)
  }

  private val decodeCommandInfo: Frame => Either[DecodeError, CommandInfo] = Decode.shape("command info entry") {
    case Frame.Array(
          Frame.BulkString(name) +: Frame.Integer(arity) +: flagsFrame +: Frame.Integer(firstKey) +: Frame.Integer(lastKey) +: Frame.Integer(
            step
          ) +: tail
        ) =>
      for {
        flags <- stringSeq(flagsFrame)
        acl   <- tail.headOption.fold[Either[DecodeError, Vector[String]]](Right(Vector.empty))(stringSeq)
      } yield CommandInfo(name.asUtf8String, arity, flags.toSet, firstKey.toInt, lastKey.toInt, step.toInt, acl.toSet)
  }

}
