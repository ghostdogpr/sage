package sage.commands

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * The reply of `FUNCTION RESTORE`'s policy argument. `Append` is the server default: add the payload's libraries, failing on a name
  * clash. `Flush` replaces all libraries with the payload; `Replace` overwrites clashing libraries and keeps the rest.
  */
enum RestorePolicy {
  case Flush, Append, Replace
}

/**
  * One callable function within a [[LibraryInfo]]. `flags` are kept as raw strings: the flag vocabulary (`no-writes`, `allow-oom`, …)
  * evolves server-side, so an unknown flag must not fail the decode.
  */
final case class FunctionInfo(name: String, description: Option[String], flags: Set[String])

/**
  * One server-stored function Library: its functions, the engine that runs them, and — only when `FUNCTION LIST … WITHCODE` was used —
  * its source.
  */
final case class LibraryInfo(libraryName: String, engine: String, functions: Vector[FunctionInfo], code: Option[String])

final case class RunningScript(name: String, command: Vector[String], duration: FiniteDuration)

final case class EngineStats(librariesCount: Long, functionsCount: Long)

final case class FunctionStats(runningScript: Option[RunningScript], engines: Map[String, EngineStats])

/**
  * The FUNCTION programmability family. `FCALL`/`FCALL_RO` return the raw RESP3 [[Frame]] like `EVAL`, since a function's reply is shaped
  * by user code. The library mutations (`LOAD`, `DELETE`, `FLUSH`, `RESTORE`) run on every master so a key-routed `FCALL` finds them.
  */
private[sage] object Functions {

  private val Load        = Bytes.utf8("LOAD")
  private val Replace     = Bytes.utf8("REPLACE")
  private val Delete      = Bytes.utf8("DELETE")
  private val Flush       = Bytes.utf8("FLUSH")
  private val Kill        = Bytes.utf8("KILL")
  private val Dump        = Bytes.utf8("DUMP")
  private val Restore     = Bytes.utf8("RESTORE")
  private val List        = Bytes.utf8("LIST")
  private val LibraryName = Bytes.utf8("LIBRARYNAME")
  private val WithCode    = Bytes.utf8("WITHCODE")
  private val Stats       = Bytes.utf8("STATS")

  def fCall(function: String): Command[Frame] = fCallCommand("FCALL", function, Vector.empty, Vector.empty, readOnly = false)

  def fCall[K](function: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    fCallCommand("FCALL", function, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = false)

  def fCall[K, V](function: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    fCallCommand("FCALL", function, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = false)

  def fCallRo(function: String): Command[Frame] = fCallCommand("FCALL_RO", function, Vector.empty, Vector.empty, readOnly = true)

  def fCallRo[K](function: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    fCallCommand("FCALL_RO", function, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = true)

  def fCallRo[K, V](function: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    fCallCommand("FCALL_RO", function, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = true)

  private def fCallCommand(name: String, function: String, keys: Vector[Bytes], args: Vector[Bytes], readOnly: Boolean): Command[Frame] = {
    val allArgs    = (Bytes.utf8(function) +: Bytes.utf8(keys.length.toString) +: keys) ++ args
    val keyIndices = Vector.range(2, 2 + keys.length)
    Command(name, keyIndices, allArgs, Decode.frame, Execution.Ordinary, isReadOnly = readOnly, cacheable = false)
  }

  def functionLoad(code: String, replace: Boolean = false): Command[String] =
    Command(
      "FUNCTION",
      Command.NoKeys,
      (Load +: (if (replace) Vector(Replace) else Vector.empty)) :+ Bytes.utf8(code),
      Decode.utf8String,
      allMasters = true
    )

  def functionDelete(libraryName: String): Command[Unit] =
    Command("FUNCTION", Command.NoKeys, Vector(Delete, Bytes.utf8(libraryName)), Decode.ok, allMasters = true)

  def functionFlush(mode: Option[FlushMode] = None): Command[Unit] =
    Command("FUNCTION", Command.NoKeys, Flush +: FlushMode.args(mode), Decode.ok, allMasters = true)

  val functionKill: Command[Unit] = Command("FUNCTION", Command.NoKeys, Vector(Kill), Decode.ok)

  val functionDump: Command[Bytes] = Command("FUNCTION", Command.NoKeys, Vector(Dump), Decode.bytes)

  def functionRestore(payload: Bytes, policy: Option[RestorePolicy] = None): Command[Unit] =
    Command(
      "FUNCTION",
      Command.NoKeys,
      Vector(Restore, payload) ++ policy.map(p => Bytes.utf8(p.toString.toUpperCase)).toVector,
      Decode.ok,
      allMasters = true
    )

  private val decodeLibraries: Frame => Either[DecodeError, Vector[LibraryInfo]] = Decode.vector(decodeLibrary)

  def functionList(libraryName: Option[String] = None, withCode: Boolean = false): Command[Vector[LibraryInfo]] =
    Command(
      "FUNCTION",
      Command.NoKeys,
      List +: (libraryName.toVector.flatMap(name => Vector(LibraryName, Bytes.utf8(name))) ++ (if (withCode) Vector(WithCode) else Vector.empty)),
      decodeLibraries
    )

  private val decodeStats: Frame => Either[DecodeError, FunctionStats] =
    frame =>
      Decode.fieldMap(frame).flatMap { fields =>
        for {
          running <- fields.get("running_script") match {
                       case None | Some(Frame.Null) => Right(None)
                       case Some(scriptFrame)       => decodeRunningScript(scriptFrame).map(Some(_))
                     }
          engines <- fields.get("engines").fold[Either[DecodeError, Map[String, EngineStats]]](Right(Map.empty))(decodeEngines)
        } yield FunctionStats(running, engines)
      }

  val functionStats: Command[FunctionStats] = Command("FUNCTION", Command.NoKeys, Vector(Stats), decodeStats)

  private def decodeLibrary(frame: Frame): Either[DecodeError, LibraryInfo] =
    Decode.fieldMap(frame).flatMap { fields =>
      for {
        name      <- requireString(fields, "library_name")
        engine    <- requireString(fields, "engine")
        functions <- fields.get("functions").fold[Either[DecodeError, Vector[FunctionInfo]]](Right(Vector.empty))(Decode.vector(decodeFunction))
      } yield LibraryInfo(name, engine, functions, optionalString(fields, "library_code"))
    }

  private def decodeFunction(frame: Frame): Either[DecodeError, FunctionInfo] =
    Decode.fieldMap(frame).flatMap { fields =>
      requireString(fields, "name").map { name =>
        FunctionInfo(name, optionalString(fields, "description"), flagSet(fields.get("flags")))
      }
    }

  private def decodeRunningScript(frame: Frame): Either[DecodeError, RunningScript] =
    Decode.fieldMap(frame).flatMap { fields =>
      for {
        name     <- requireString(fields, "name")
        command  <- fields.get("command").fold[Either[DecodeError, Vector[String]]](Right(Vector.empty))(Decode.vector(Decode.utf8String))
        duration <- fields.get("duration_ms").fold[Either[DecodeError, Long]](Right(0L))(Decode.long)
      } yield RunningScript(name, command, duration.millis)
    }

  private def decodeEngines(frame: Frame): Either[DecodeError, Map[String, EngineStats]] =
    Decode.fieldMap(frame).flatMap { engines =>
      engines.foldLeft[Either[DecodeError, Map[String, EngineStats]]](Right(Map.empty)) { case (acc, (name, statsFrame)) =>
        for {
          map    <- acc
          fields <- Decode.fieldMap(statsFrame)
        } yield map + (name -> EngineStats(
          fields.get("libraries_count").flatMap(asLong).getOrElse(0L),
          fields.get("functions_count").flatMap(asLong).getOrElse(0L)
        ))
      }
    }

  private def requireString(fields: Map[String, Frame], key: String): Either[DecodeError, String] =
    fields.get(key).flatMap(asString).toRight(DecodeError(s"'$key' field", s"map without '$key'"))

  private def optionalString(fields: Map[String, Frame], key: String): Option[String] =
    fields.get(key).flatMap(asString)

  private def asString(frame: Frame): Option[String] =
    frame match {
      case Frame.BulkString(b)   => Some(b.asUtf8String)
      case Frame.SimpleString(s) => Some(s)
      case _                     => None
    }

  private def asLong(frame: Frame): Option[Long] =
    frame match {
      case Frame.Integer(v) => Some(v)
      case _                => None
    }

  private def flagSet(frame: Option[Frame]): Set[String] =
    frame match {
      case Some(Frame.Set(elements))   => elements.flatMap(asString).toSet
      case Some(Frame.Array(elements)) => elements.flatMap(asString).toSet
      case _                           => Set.empty
    }
}
