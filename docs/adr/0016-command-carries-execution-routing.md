# Commands carry their own execution routing

A blocking command (`BLPOP`, `BLMPOP`, …) must run on a Dedicated Connection so it never stalls the Multiplexed Connection, while ordinary commands are auto-pipelined onto the multiplexed one. The client's `run` must choose the path, and to keep that choice transparent (the caller never picks a connection) it has to read the requirement off the command itself. The pure core `Command` value therefore gains an `execution: Execution` field — `enum Execution { Ordinary, Blocking }`, defaulting to `Ordinary` — and `run` branches on it: `Ordinary` submits to the Multiplexed Connection, `Blocking` borrows from the dedicated pool.

This puts a hint about a *runtime* connection choice inside the sans-IO core (ADR-0001), which is the surprising part and the reason it is recorded. It is justified because the routing requirement is intrinsic to the command, not to any I/O: `BLPOP` blocks the connection wherever it runs, on every backend, so the fact belongs to the command's pure description, not to per-backend runtime glue. The flag is bare data — no behavior, no I/O — so the zero-dependency and sans-IO constraints are untouched.

## Considered Options

- **Field on `Command` (chosen)** — the command declares its own requirement; `run` is a two-line match. Pure data; backends share one routing rule.
- **Name lookup in the runtime** — a `Set` of blocking command names matched against `command.name`. No core change, but a hidden table that silently drifts from the command set and leaks routing into a string match.
- **A separate `Client` method** (`runDedicated`) — forfeits transparency: the caller must know which path a command needs, exactly what the glossary's "transparent to callers" rules out.

## Consequences

- The marker names the *reason* (`Blocking`), not the connection kind, and carries no payload — a blocking command's server timeout stays in `args`, the runtime does not branch on it.
- Transactions also need a Dedicated Connection but are deliberately **not** an `Execution` case: `WATCH`/`MULTI`/`EXEC` pin one connection across many `run` calls, which a per-command flag cannot express. They will arrive as a separate leased-connection API, not a third `Execution` value.
