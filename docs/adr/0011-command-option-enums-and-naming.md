# Command option enums, naming rules, and the family pattern

Command options are modeled as small per-command Scala enums passed as defaulted parameters (`SetExpiry`, `SetCondition`, `GetExpiry`, `ExpireCondition`), not fluent builders (Lettuce/Jedis) or overload dispatch (zio-redis): one enum value per mutually-exclusive option group makes illegal combinations (`EX` + `KEEPTTL`, `NX` + `XX`) unrepresentable, and enums are never shared across commands — each mirrors exactly its own command's legal option space. An option that changes the reply type becomes a separate builder instead (`SET ... GET` is `setGet: Command[Option[V]]` next to `set: Command[Boolean]`), keeping `Out` concrete rather than type-level.

Naming is mechanical: camelCase per wire word (`pTtl`, `mGet`, `renameNx`), the convention only zio-redis applies consistently; `typeOf` for TYPE (keyword); `setGet` follows the ecosystem-unanimous name. Multi-key commands take `(first: K, rest: K*)` so zero-key calls don't compile. Time is typed — `FiniteDuration`/`Instant` in, ADTs out (`Ttl.NoKey | NoExpiry | Expires`) — and sub-second precision selects the millisecond wire variant (`expire` emits `EXPIRE` or `PEXPIRE`), so the wire form is an encoding detail rather than a method choice.

## Consequences

- Every remaining family copies this shape mechanically; deviations need a reason.
- Family behavior suites and the Coverage Spec run in one designated backend cell against both servers — per-command behavior cannot differ across backends, and the per-cell smoke suites already patrol the lowering boundary. Only genuinely per-backend surface (e.g. `scanAll` stream sugar) is tested per backend.
