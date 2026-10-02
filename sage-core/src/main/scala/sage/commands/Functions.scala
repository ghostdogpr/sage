package sage.commands

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.Replace
import sage.commands.KeyArgs.ScriptVerb
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
  * One function library stored on the server. It contains the functions, their engine, and the source code when the caller used
  * `FUNCTION LIST WITHCODE`.
  */
final case class LibraryInfo(libraryName: String, engine: String, functions: Vector[FunctionInfo], code: Option[String])

/**
  * The function or script currently executing, as reported by `FUNCTION STATS`: its name, the command line, and how long it has run.
  */
final case class RunningScript(name: String, command: Vector[String], duration: FiniteDuration)

/**
  * Per-engine totals from `FUNCTION STATS`: how many libraries and functions an engine holds.
  */
final case class EngineStats(librariesCount: Long, functionsCount: Long)

/**
  * A `FUNCTION STATS` reply: the [[RunningScript]] if one is executing, and per-engine [[EngineStats]] keyed by engine name.
  */
final case class FunctionStats(runningScript: Option[RunningScript], engines: Map[String, EngineStats])

/**
  * The FUNCTION programmability family. `FCALL`/`FCALL_RO` return the raw RESP3 [[Frame]] like `EVAL`, since a function's reply is shaped
  * by user code. The library mutations (`LOAD`, `DELETE`, `FLUSH`, `RESTORE`) run on every master so a key-routed `FCALL` finds them.
  */
private[sage] object Functions {

  private val Load        = Bytes.utf8("LOAD")
  private val Delete      = Bytes.utf8("DELETE")
  private val Flush       = Bytes.utf8("FLUSH")
  private val Kill        = Bytes.utf8("KILL")
  private val Dump        = Bytes.utf8("DUMP")
  private val Restore     = Bytes.utf8("RESTORE")
  private val List        = Bytes.utf8("LIST")
  private val LibraryName = Bytes.utf8("LIBRARYNAME")
  private val WithCode    = Bytes.utf8("WITHCODE")
  private val Stats       = Bytes.utf8("STATS")
  private val policyArg   = Args.keywords(RestorePolicy.values)

  def fCall(function: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.FCall, function, Seq.empty[Bytes], Seq.empty[Bytes])

  def fCall[K](function: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.FCall, function, keys, Seq.empty[Bytes])

  def fCall[K, V](function: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.FCall, function, keys, args)

  def fCallRo(function: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.FCallRo, function, Seq.empty[Bytes], Seq.empty[Bytes])

  def fCallRo[K](function: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.FCallRo, function, keys, Seq.empty[Bytes])

  def fCallRo[K, V](function: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.FCallRo, function, keys, args)

  def functionLoad(code: String, replace: Boolean = false): Command[String] =
    Command(
      "FUNCTION",
      Command.NoKeys,
      (Load +: Args.flag(replace, Replace)) :+ Bytes.utf8(code),
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
      Vector(Restore, payload) ++ policy.map(policyArg).toVector,
      Decode.ok,
      allMasters = true
    )

  def functionList(libraryName: Option[String] = None, withCode: Boolean = false): Command[Vector[LibraryInfo]] =
    Command(
      "FUNCTION",
      Command.NoKeys,
      List +: (Args.optText(LibraryName, libraryName) ++ Args.flag(withCode, WithCode)),
      Decode.vector(decodeLibrary)
    )

  private val decodeStats: Frame => Either[DecodeError, FunctionStats] =
    Decode.fields { f =>
      for {
        running <- f.optional("running_script", decodeRunningScript)
        engines <- f.requiredOr("engines", decodeEngines, Map.empty)
      } yield FunctionStats(running, engines)
    }

  val functionStats: Command[FunctionStats] = Command("FUNCTION", Command.NoKeys, Vector(Stats), decodeStats)

  private val decodeLibrary: Frame => Either[DecodeError, LibraryInfo] =
    Decode.fields { f =>
      for {
        name      <- f.required("library_name", Decode.text)
        engine    <- f.required("engine", Decode.text)
        functions <- f.optionalVector("functions", decodeFunction)
        code      <- f.optional("library_code", Decode.text)
      } yield LibraryInfo(name, engine, functions, code)
    }

  private val decodeFunction: Frame => Either[DecodeError, FunctionInfo] =
    Decode.fields { f =>
      for {
        name        <- f.required("name", Decode.text)
        description <- f.optional("description", Decode.text)
      } yield FunctionInfo(name, description, flagSet(f.get("flags")))
    }

  private val decodeRunningScript: Frame => Either[DecodeError, RunningScript] =
    Decode.fields { f =>
      for {
        name     <- f.required("name", Decode.text)
        command  <- f.optionalVector("command", Decode.utf8String)
        duration <- f.requiredOr("duration_ms", Decode.long, 0L)
      } yield RunningScript(name, command, duration.millis)
    }

  private val decodeEngines: Frame => Either[DecodeError, Map[String, EngineStats]] =
    Decode.fieldValues(Decode.fields { stats =>
      for {
        libraries <- stats.requiredOr("libraries_count", Decode.long, 0L)
        functions <- stats.requiredOr("functions_count", Decode.long, 0L)
      } yield EngineStats(libraries, functions)
    })

  private def flagSet(frame: Option[Frame]): Set[String] =
    frame match {
      case Some(Frame.Set(elements))   => elements.flatMap(Decode.text(_).toOption).toSet
      case Some(Frame.Array(elements)) => elements.flatMap(Decode.text(_).toOption).toSet
      case _                           => Set.empty
    }
}
