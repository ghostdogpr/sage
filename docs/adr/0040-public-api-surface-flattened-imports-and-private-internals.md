# Public API surface: flattened `sage.*` vocabulary and `private[sage]` internals

The public surface is frozen behind two wildcard imports: `import sage.*` brings the whole command vocabulary and the connection config, and `import sage.<backend>.*` (zio/ce/ox/kyo) brings the client. A hello-world — connect, GET/SET, a blocking pop, a pipeline, an `eval` — needs only those two. The layered packages (`sage.commands`, `sage.codec`, `sage.protocol`, `sage.cluster`) remain the code's organization, not the user's; `import sage.*` is the user's.

## How the vocabulary is surfaced

`sage.*` is a set of top-level `export` clauses in one file — the command facade `Commands`, `Command`, `Execution`, `Pipeline` and its `pipeline` extension, every option/result enum and ADT, `Frame` with the `FrameDecode` extensions (`as`, `asArray`, …) named individually, the `KeyCodec`/`ValueCodec` typeclasses, `Node`, and the connection config (`SageConfig`, `Endpoint`, `Topology`, `ReadFrom`, …). A package cannot be wildcard-exported (`export sage.commands.*` does not compile, top-level or inside an object), so the surface is enumerated — which also makes it the one place the public API is reviewed. Forgetting to add a new public type there is the maintenance risk; a completeness check is a candidate follow-up.

Two Scala constraints shaped the form:

- **Exactly one module may contribute top-level members to `package sage`.** A package's top-level `export` forwarders are silently dropped from a wildcard import when a *second* module also contributes top-level members to the same package and a subpackage of it is wildcard-imported in the same scope (`import sage.*` + `import sage.zio.*`); `Bytes` and the other real classes in `package sage` survive, but the export forwarders do not. Core's `package sage` holds only type *definitions* (direct package members, no synthetic `sage$package` holder), so the aggregator lives in the **shared client module** as the sole `sage$package`, and carries both the (core) vocabulary and the (client) config in one file — config is backend-independent and a user always depends on a backend. This is why config is not re-exported per backend, and why the aggregator is not in core.

- **`import sage.*` brings the subpackage names** (`zio`, `commands`, …), so a *qualified* `zio.Duration` resolves to `sage.zio` and fails; use the unqualified form (from `import zio.*`). Ordinary unqualified usage is unaffected; this only bites code that qualifies with a name that collides with a sage subpackage.

An aggregator `object` (`import sage.api.*`) was prototyped to dodge the first constraint but rejected: the headline import should be `sage.*`, and the single-`sage$package` rule recovers it.

## What is hidden, and the constraint that pins what cannot be

Types shared core→client cannot be fully `private`, so `private[sage]` hides them from users while preserving cross-module access. `private[sage]` is the established core mechanism (already pervasive: the per-family builder objects, `Decode`, `RespWriter`). Newly hidden: `RespParser`, `Reply`, `HelloReply`, and the cluster-routing internals (`ClusterTopology`, `Shard`, `SlotRange`, `Route`, `Redirect`, `RedirectKind`, `SplitPlan`, `NodeGroup`, `Rejected`, `Slot`). The per-family objects (`Strings`/`Keys`/…) stay `private[sage]`; `Commands` re-exports their members, so `Commands.get` is public while `Strings` is not importable.

Three types look like internals but are pinned public by existing design and must stay exposed:

- **`Frame`** (trait, cases, and the `FrameDecode` extension) — `eval`/`fcall` return `Command[Frame]` whose reply shape is defined by user code (ADR-0033); the cases stay public so a caller can pattern-match an arbitrary script reply, not only decode it through `.as[A]`. It is also a public field of the public `Command` (`decode: Frame => …`).
- **`Node`** — surfaced by the Listener SPI through `SageEvent.CommandCompleted`, `Connected`/`Disconnected`, and `TopologyChanged(masters: Vector[Node])` (ADR-0035), so a Listener author must be able to name it. It is the only `sage.cluster` type that stays public.
- **`Execution`** — a public field of the public `Command`.

## Considered Options

- **Keep layered** (`sage.commands.*` canonical) — rejected: it forces users to mirror the library's internal organization for every call, with no payoff.
- **Flatten per-backend** (`import sage.zio.*` brings everything) — rejected: it dumps the entire vocabulary into each effect ecosystem's namespace (collision surface, unclear provenance) and duplicates the re-export across four backends.
- **Aggregator `object` (`import sage.api.*`)** — rejected once the single-module rule recovered plain `import sage.*`.
- **`internal` subpackages instead of `private[sage]`** — rejected: a package named `internal` does not make types non-importable (only an access modifier does), it is absent from `sage-core` today, and core's internals are interleaved with public vocabulary in the same files.
