# Pipeline result model: per-position truth, strict tuple as sugar

A Pipeline sends N commands in one round-trip; the server replies per command, and one command erroring (a `WRONGTYPE`, a decode failure) is routine and must not hide the other replies. So the truthful result is a per-position outcome — each slot a `Right(value)` or a `Left(SageException)` — exactly as rueidis, ioredis, Jedis, lettuce and StackExchange.Redis all expose it. But the headline ergonomic the glossary promises is a *typed tuple*, which only reads well when every command succeeds. We keep both: `pipelineAttempt` returns the per-position results (the primitive), and `pipeline` is sugar that unwraps to a clean tuple when all succeed and otherwise fails the effect with the first error.

The pure `Pipeline[Out, Results]` carries two type parameters — the all-success shape and the per-position shape — because neither construction form can recover the other from `Out` alone: the tuple builder maps `(A, B, C)` to `(Either[E, A], Either[E, B], Either[E, C])` via `Tuple.Map`, while `sequence` maps `Vector[A]` to `Vector[Either[E, A]]`. The runtime decodes each position independently into a `Vector[Either[SageException, Any]]` and either collapses it (strict) or shapes it (attempt); the `Any` is fenced behind the typed builders and never reaches a caller.

## Considered Options

- **Per-position primitive + strict sugar (chosen)** — honest about partial failure and keeps the common all-success call site clean. The only modern client chasing a single typed tuple, redis4cats, pays for it with all-or-nothing `Option[HList]`; everyone facing real Redis semantics exposes per-position.
- **Fail-fast tuple only** — `F[(A, B, C)]` failing wholesale on any error. Cleanest type, but discards the surviving replies and so fails the acceptance criterion.
- **Per-position only** — forces every call site to pattern-match `Either`s even when all succeed; poor ergonomics for the headline API.

## Consequences

- The strict path fails with the **first** error, not an aggregate; the sealed `SageException` hierarchy gains no case, and a caller who wants every failure uses `pipelineAttempt`.
- A blocking command in a Pipeline would stall the Multiplexed Connection. Making that unrepresentable would mean lifting `Execution` into the `Command` type — superseding ADR-0016 and complicating `Command.map` for every command — so it is instead rejected at the effect boundary: the pure builder stays total, and `pipeline`/`pipelineAttempt` fail the effect with an `IllegalArgumentException` (a programmer error, deliberately outside the sealed hierarchy).
- `pipeline` and `pipelineAttempt` are primitives on `Client` alongside `run`, not sugar over it — a Pipeline cannot be expressed as repeated `run` without losing the single round-trip, and the strict/attempt collapse needs effect capability the bare trait lacks. A fake therefore implements three primitives, not one.
- The Pipeline retains each command's `keyIndices`, so cluster per-node split/merge (a later slice) can route it without reshaping the type. This slice executes only over the single standalone Multiplexed Connection.
