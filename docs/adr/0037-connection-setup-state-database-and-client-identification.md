# Database and client identification are connection-setup state

The configured `database`, the optional `clientName`, and the library identification (`LIB-NAME`/`LIB-VER`) are applied during the `HELLO` bootstrap that every connection runs on connect and re-runs on reconnect — the same path as `AUTH` and `CLIENT TRACKING` — and never as runtime commands. The bootstrap, after `HELLO`, issues `CLIENT SETINFO LIB-NAME sage`, `CLIENT SETINFO LIB-VER <version>`, an optional `CLIENT SETNAME`, and `SELECT <database>` when non-zero. Because these sit in the shared bootstrap vector, the Multiplexed Connection, every Dedicated Connection from the pool, and the Subscription Connection all get them, and a reconnect restores them (story 22).

`SELECT` is config-only and deliberately not a runtime `client.select(n)`: the Multiplexed Connection is shared by all fibers, so a per-call `SELECT` would move the database under everyone mid-flight. The same reasoning already keeps `AUTH`/`RESET`/`SWAPDB` off the runtime surface (see the coverage skips). Pinning the database once at setup is the safe model; a wrong database is a correctness hazard, so `SELECT` stays strict (a failure fails the connection). Cluster has only database 0, so a non-zero `database` is rejected in config validation for a cluster topology, and cluster URIs reject a `/db` path.

`LIB-VER` is sourced from the build version via `sbt-buildinfo` (`sage.client.BuildInfo.version`, fed by `sbt-dynver`), generated into the client module so the zero-dependency core (ADR-0004) stays untouched — the core only gains a pure `Connection.clientSetInfo(attribute, value)` builder that takes plain strings.

## Considered Options

- **A runtime `client.select`** — rejected: switches the database for every fiber sharing the connection.
- **Pin the database on the Multiplexed Connection only** — rejected: blocking commands (on a Dedicated Connection) and transactions would then run against a different database than ordinary commands.
- **Connection-setup, all connections, re-applied on reconnect (chosen).**

## Consequences

Identification is strict like `CLIENT TRACKING`: a server that denies `CLIENT SETINFO` fails the connection. This is acceptable because sage targets Redis/Valkey 8+, where `CLIENT SETINFO` (≥7.2) is always available; if a restrictive proxy ever makes this a problem, making identification best-effort is a contained follow-up. Operators now see `sage` and its version in `CLIENT LIST`/`CLIENT INFO`. The `SELECT` command stays in the coverage skip list, with its reason updated from "unsupported" to "issued at connection setup, not a runnable command".
