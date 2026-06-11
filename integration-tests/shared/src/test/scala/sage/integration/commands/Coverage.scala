package sage.integration.commands

/**
  * The acknowledged command-coverage partition. Every server-reported command is either implemented (via a [[CommandSamples]] sample) or
  * listed in `skipped` (deliberate-never, with a reason); the coverage spec fails on any unacknowledged drift. Subcommands modeled as
  * arguments under a bare command are not listed here — the spec drops space-containing server names that sage does not implement as a name
  * of their own.
  */
object Coverage {

  val skipped: Map[String, String] = Map(
    "GETSET"               -> "deprecated: SET with setGet subsumes it",
    "SETNX"                -> "deprecated: SET with SetCondition.IfNotExists",
    "SETEX"                -> "deprecated: SET with SetExpiry.In",
    "PSETEX"               -> "deprecated: SET with SetExpiry.In",
    "SUBSTR"               -> "deprecated: GETRANGE",
    "HMSET"                -> "deprecated: HSET takes multiple field/value pairs",
    "RPOPLPUSH"            -> "deprecated: LMOVE",
    "ZRANGEBYSCORE"        -> "deprecated: ZRANGE with ByScore",
    "ZRANGEBYLEX"          -> "deprecated: ZRANGE with ByLex",
    "ZREVRANGE"            -> "deprecated: ZRANGE with ByRank and rev",
    "ZREVRANGEBYSCORE"     -> "deprecated: ZRANGE with ByScore and rev",
    "ZREVRANGEBYLEX"       -> "deprecated: ZRANGE with ByLex and rev",
    "BRPOPLPUSH"           -> "deprecated: BLMOVE",
    "GEORADIUS"            -> "deprecated: GEOSEARCH/GEOSEARCHSTORE",
    "GEORADIUS_RO"         -> "deprecated: GEOSEARCH",
    "GEORADIUSBYMEMBER"    -> "deprecated: GEOSEARCH/GEOSEARCHSTORE",
    "GEORADIUSBYMEMBER_RO" -> "deprecated: GEOSEARCH",
    "SUBSCRIBE"            -> "delivered via the subscribe stream API, not a runnable command",
    "PSUBSCRIBE"           -> "delivered via the pSubscribe stream API, not a runnable command",
    "SSUBSCRIBE"           -> "delivered via the sSubscribe stream API, not a runnable command",
    "UNSUBSCRIBE"          -> "sent by the subscribe stream's scope closure, not a runnable command",
    "PUNSUBSCRIBE"         -> "sent by the pSubscribe stream's scope closure, not a runnable command",
    "SUNSUBSCRIBE"         -> "sent by the sSubscribe stream's scope closure, not a runnable command",
    "MULTI"                -> "driven by the transaction scope API, not a runnable command",
    "EXEC"                 -> "driven by the transaction scope API, not a runnable command",
    "WATCH"                -> "driven by the transaction scope API, not a runnable command",
    "UNWATCH"              -> "driven by the transaction scope API, not a runnable command",
    "DISCARD"              -> "the transaction scope abandons by dropping its connection, not via DISCARD",
    "ASKING"               -> "issued by cluster ASK-redirect handling, not a runnable command",
    "RESTORE-ASKING"       -> "issued by cluster ASK-redirect handling during slot migration, not a runnable command",
    "XGROUP"               -> "subcommand container; each XGROUP subcommand is its own Command name",
    "XINFO"                -> "subcommand container; each XINFO subcommand is its own Command name",
    "XIDMPRECORD"          -> "internal: replayed during AOF loading, not for client use",
    "PFDEBUG"              -> "internal HyperLogLog debugging command, out of scope",
    "PFSELFTEST"           -> "internal HyperLogLog self-test command, out of scope",
    "PSYNC"                -> "replication protocol between servers, never issued by a client",
    "SYNC"                 -> "replication protocol between servers, never issued by a client",
    "REPLCONF"             -> "replication protocol between servers, never issued by a client",
    "REPLICAOF"            -> "server replication administration, out of scope",
    "SLAVEOF"              -> "deprecated replication administration (REPLICAOF), out of scope",
    "FAILOVER"             -> "server failover administration, out of scope",
    "SHUTDOWN"             -> "server lifecycle administration, out of scope",
    "MONITOR"              -> "debugging firehose that hijacks the connection, out of scope",
    "DEBUG"                -> "server debugging command, out of scope",
    "LOLWUT"               -> "easter-egg command, no client API",
    "SELECT"               -> "changing the selected DB on the shared Multiplexed Connection would affect all fibers; unsupported",
    "SWAPDB"               -> "whole-database administration, out of scope",
    "QUIT"                 -> "deprecated: close the client instead of sending QUIT",
    "RESET"                -> "would reset state on the shared Multiplexed Connection across all fibers; not exposed",
    "AUTH"                 -> "established at handshake via HELLO AUTH; re-auth on the shared Multiplexed Connection would affect all fibers",
    "READONLY"             -> "replica-read routing is managed internally, not toggled per command on the shared connection",
    "READWRITE"            -> "replica-read routing is managed internally, not toggled per command on the shared connection",
    "MODULE"               -> "loadable modules are out of scope",
    "CLUSTERSCAN"          -> "cluster operator administration; sage consumes topology internally and is not a cluster-admin tool",
    "TRIMSLOTS"            -> "cluster operator administration; sage consumes topology internally and is not a cluster-admin tool",
    "SAVE"                 -> "persistence administration; synchronous save blocks the server, operator/cron tooling not an app-client concern",
    "BGSAVE"               -> "persistence administration; backup is operator/cron tooling, not an app-client concern",
    "BGREWRITEAOF"         -> "persistence administration; AOF rewrite is operator/cron tooling, not an app-client concern",
    "LASTSAVE"             -> "persistence introspection paired with SAVE/BGSAVE, which are operator tooling",
    "HOTKEYS"              -> "hot-key profiling session (START/STOP/GET/RESET); operator diagnostic tooling like MONITOR, not exposed"
  )
}
