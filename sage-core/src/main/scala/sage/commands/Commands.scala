package sage.commands

/**
  * The public builder facade: every command as a [[Command]] value (`Commands.get`, `Commands.incr`, …), named one-for-one with the
  * client's per-command methods. This is the path for `run`, pipelines, transactions, and reuse; the client sugar (e.g. `client.get`) is a
  * thin wrapper over the same values. Each member is re-exported from an internal per-family object, so it appears here as `Exported from …`
  * with its signature; the prose for what a command does and its contracts lives on the matching client method.
  */
object Commands {
  export Acl.*
  export Arrays.*
  export Bitmaps.*
  export Connection.{clientGetName, clientGetRedir, clientId, clientInfo, clientList, echo, ping}
  export Functions.*
  export Geo.*
  export Hashes.*
  export HyperLogLog.*
  export Json.*
  export Keys.*
  export Lists.*
  export Pubsub.{publish, pubsubChannels, pubsubNumPat, pubsubNumSub, pubsubShardChannels, pubsubShardNumSub, sPublish}
  export Scripting.*
  export Server.*
  export Sets.*
  export SortedSets.*
  export StreamInfo.*
  export Streams.*
  export Strings.*
}
