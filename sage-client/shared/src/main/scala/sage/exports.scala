package sage

// `import sage.*` provides backend-independent commands, models, options, results, raw `Frame` decoding, codecs, and connection configuration.
// The backend client comes from `import sage.backend.*`, using the same import for every effect system.
//
// Keep these top-level exports in the shared client module. If another module contributes top-level members to package `sage`, combining
// `import sage.*` with `import sage.backend.*` can silently omit those members. Core defines only types directly in package `sage`, leaving
// this file as the only generated `sage$package` holder.
//
// Scala cannot wildcard-export a package, so each public name is listed here. New public option and result types must be added below to
// become available through `sage.*`.

export sage.client.{
  AuthConfig,
  BackoffConfig,
  CacheConfig,
  ClusterConfig,
  DedicatedPoolConfig,
  Endpoint,
  MasterReplicaConfig,
  PubSubConfig,
  ReadFrom,
  SageConfig,
  TlsConfig,
  Topology,
  TrustSource,
  WatchdogConfig
}
export sage.client.RateLimiterClient
// the cluster node a Listener observes (SageEvent), the only cluster type users name
export sage.cluster.Node
// codec typeclasses (built-in givens live in their companions, already in implicit scope)
export sage.codec.{KeyCodec, ValueCodec}
export sage.commands.{
  AclLogEntry,
  AclUser,
  Aggregate,
  ArGrepCombine,
  ArMatch,
  ArrayInfo,
  ArrayInfoFull,
  BitFieldOffset,
  BitFieldOp,
  BitFieldOverflow,
  BitFieldType,
  BitPosRange,
  BitRange,
  BitUnit,
  BlockTimeout,
  ClaimIdle,
  CommandFilterBy,
  CommandHistogram,
  CommandInfo,
  CommandLogEntry,
  CommandLogType,
  ConsumerInfo,
  DelexCondition,
  EngineStats,
  ExpireCondition,
  ExpiryTime,
  FieldExpiry,
  FieldExpiryTime,
  FieldPersist,
  FieldTtl,
  FlushMode,
  FullConsumerInfo,
  FullGroupInfo,
  FullPendingEntry,
  FunctionInfo,
  FunctionStats,
  GeoAddCondition,
  GeoCoordinates,
  GeoCount,
  GeoOrigin,
  GeoSearchResult,
  GeoShape,
  GeoSort,
  GeoUnit,
  GetExpiry,
  GroupInfo,
  GroupReadId,
  GroupStartId,
  HSetExCondition,
  IncrExpiry,
  IncrExResult,
  InsertPosition,
  JsonPath,
  JsonSetCondition,
  JsonType,
  LatencyEntry,
  LcsMatch,
  LcsMatches,
  LexBoundary,
  LibraryInfo,
  Limit,
  ListSide,
  MatchRange,
  MigrateAuth,
  MigrateResult,
  MinMax,
  NackMode,
  PendingEntry,
  PendingSummary,
  ReadId,
  RedisType,
  ReplicaNode,
  RestoreExpiry,
  RestorePolicy,
  Role,
  RunningScript,
  ScanCursor,
  ScanPage,
  ScoreBoundary,
  SetCondition,
  SetExpiry,
  SlowLogEntry,
  SortOrder,
  StreamDeletionPolicy,
  StreamEntry,
  StreamEntryDeletion,
  StreamId,
  StreamInfo,
  StreamInfoFull,
  StreamRangeId,
  Trimming,
  TrimThreshold,
  Ttl,
  XAddId,
  XAutoClaimJustIdResult,
  XAutoClaimResult,
  ZAddCondition,
  ZRange
}
export sage.commands.{as, asArray, asArrayOf, asLong, asString}
export sage.commands.{Attempt, BroadcastReduce, Command, Commands, Execution}
// raw-Frame escape hatch for eval/fcall replies
export sage.protocol.Frame
// built-in token-bucket rate limiter
export sage.ratelimit.{Decision, RateLimit, RateLimiter}
