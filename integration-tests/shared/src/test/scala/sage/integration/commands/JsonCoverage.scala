package sage.integration.commands

/**
  * The acknowledged coverage partition for the JSON extension module, kept separate from [[Coverage]] because JSON commands come from a
  * loadable module the core spec subtracts. Every JSON command a module-bearing server reports, including each `JSON.DEBUG` subcommand, is
  * either implemented (a [[sage.commands.CommandSamples]] sample) or listed here with a reason. The two servers report different command
  * sets: RedisJSON adds `MERGE` and `NUMPOWBY` ([[redisOnly]]), while valkey-json enumerates every `JSON.DEBUG` subcommand where RedisJSON
  * reports only the bare container ([[valkeyOnly]]).
  */
object JsonCoverage {

  val redisOnly: Set[String] = Set("JSON.MERGE", "JSON.NUMPOWBY")

  val valkeyOnly: Set[String] = Set(
    "JSON.DEBUG DEPTH",
    "JSON.DEBUG FIELDS",
    "JSON.DEBUG HELP",
    "JSON.DEBUG KEYTABLE-CHECK",
    "JSON.DEBUG KEYTABLE-CORRUPT",
    "JSON.DEBUG KEYTABLE-DISTRIBUTION",
    "JSON.DEBUG MAX-DEPTH-KEY",
    "JSON.DEBUG MAX-SIZE-KEY",
    "JSON.DEBUG MEMORY",
    "JSON.DEBUG TEST-SHARED-API"
  )

  val skipped: Map[String, String] = Map(
    "JSON.FORGET"                      -> "alias of JSON.DEL",
    "JSON.NUMPOWBY"                    -> "niche exponentiation, Redis-only with no Valkey equivalent",
    "JSON.DEBUG"                       -> "subcommand container; JSON.DEBUG MEMORY is modeled as its own Command name",
    "JSON.DEBUG DEPTH"                 -> "diagnostic subcommand, out of scope",
    "JSON.DEBUG FIELDS"                -> "diagnostic subcommand, out of scope",
    "JSON.DEBUG HELP"                  -> "help text, not a runnable operation",
    "JSON.DEBUG KEYTABLE-CHECK"        -> "internal keytable diagnostic, out of scope",
    "JSON.DEBUG KEYTABLE-CORRUPT"      -> "internal keytable diagnostic, out of scope",
    "JSON.DEBUG KEYTABLE-DISTRIBUTION" -> "internal keytable diagnostic, out of scope",
    "JSON.DEBUG MAX-DEPTH-KEY"         -> "internal diagnostic, out of scope",
    "JSON.DEBUG MAX-SIZE-KEY"          -> "internal diagnostic, out of scope",
    "JSON.DEBUG TEST-SHARED-API"       -> "internal test hook, out of scope"
  )
}
