package sage.commands

/**
  * Provides every command as a [[Command]] value, such as `Commands.get` and `Commands.incr`. These values can be passed to `run`, included
  * in pipelines and transactions, or reused. Methods such as `client.get` build the same commands. Members are re-exported from internal
  * command-family objects, which is why the generated API documentation marks them as `Exported from …`. Detailed command documentation
  * appears on the corresponding client method.
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
