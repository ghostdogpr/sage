package sage

// `import sage.*` — the one import for everything backend-independent: the command vocabulary (facade, model, options, results, raw-`Frame`
// decode, codecs) and the connection config. The backend client comes from `import sage.backend.*` (the same import for every effect system).
//
// These top-level `export` clauses live in the shared client module, and it is the ONLY module that contributes top-level members to
// package `sage`: such members are silently dropped from a wildcard import when a SECOND module also contributes them and a subpackage is
// wildcard-imported in the same scope (`import sage.*` + `import sage.backend.*`). Core's `package sage` holds only type definitions (direct
// package members, no synthetic holder), so this file is the sole `sage$package` — vocabulary and config can therefore sit together here.
//
// A package cannot be wildcard-exported, so the surface is enumerated here — which also makes this the one place the public API is reviewed;
// a new public option/result type must be added below to appear under `sage.*`.

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
