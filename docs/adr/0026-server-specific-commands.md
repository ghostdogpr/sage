# Support server-specific (Redis-only) commands

Redis 8.x ships commands that Valkey 8.1 does not have: the hash field-expiration family (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`, `HTTL`, `HPTTL`, `HGETDEL`, `HGETEX`, `HSETEX`) and a set of string extras (`DIGEST`, `DELEX`, `INCREX`, `MSETEX`). They are standard, documented Redis commands — not experimental or module-provided — so we implement them as ordinary `Command` builders in their natural family objects (`Hashes`, `Strings`), test them in dedicated single-server integration suites that run only against Redis (no Valkey counterpart), and rely on the coverage spec diffing against the *union* of both servers' reported commands to keep them acknowledged rather than flagged as drift.

## Considered Options

- **Leave in `todo`** — honest (they are real gaps) but the commands are useful and we want them now, and the issue driving this work asked not to leave them unaddressed.
- **Move to `skipped`** — overloads `skipped`, which means *deliberate-never* (deprecated, admin, replication, subcommand-as-argument). A command we intend to support but haven't is an acknowledged gap (`todo`), not a never.
- **Implement (chosen)** — they are legitimate commands; the only cost is a test-placement pattern for commands one server lacks.

## Consequences

The "one abstract family suite, two server subclasses" pattern no longer holds universally: a command present on only one server gets a single-server suite (`RedisHashFieldExpirySuite`, `RedisStringExtrasSuite`) with no counterpart for the server that lacks it. The coverage partition stays exact because `implemented` is a subset of the server union — a Redis-only command is covered by Redis's side of that union. If a future Valkey release adds these commands, their tests can move into the shared cross-server suites with no change to the builders.
