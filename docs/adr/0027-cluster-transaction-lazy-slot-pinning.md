# Cluster transactions pin lazily to the slot of their first key

A cluster Transaction is the same leased-scope API as standalone (ADR-0020), but it holds no connection at scope start: the Dedicated Connection is acquired lazily on the first operation that touches a key (`watch`, a keyed read, or `exec`), pinned to the slot's owning node, and every later key — in any `watch`, read, or the `exec` pipeline — must hash to that same slot or fail with the sealed `CrossSlot` error. We pin lazily rather than ask the caller to declare a slot up front because the interactive read-modify-write the scope exists for (ADR-0020) does not know its slot until it touches a key, and forcing a slot parameter would leak cluster topology into a call that ADR-0007 keeps topology-agnostic. The unit is the slot, not the node: Redis rejects a `MULTI`/`EXEC` whose keys span slots even within one node, so pinning to a node would let cross-slot transactions through to a server-side `EXEC` error instead of failing fast.

## Considered Options

- **Lazy pin on the first key (chosen)** — matches how an interactive RMW actually flows, keeps the API identical to standalone, and fails cross-slot fast on the client.
- **Require a slot/key up front** (`transaction(slot) { … }`) — deterministic and acquires eagerly like standalone, but leaks topology into the call site, contradicting ADR-0007, and is redundant when the first key already names the slot.
- **Pin to the node, not the slot** — fewer false rejections (two slots on one node would pass), but lets a transaction Redis will reject reach the wire, defeating the fail-fast acceptance criterion.

## Consequences

- Validation has two points, both raising `CrossSlot`: each `watch`/keyed read is checked as issued (a mismatch sends nothing for it, while earlier same-slot reads have legitimately run — honest RMW behavior), and `exec` validates the whole pipeline against the pin *before* sending `MULTI`, so the atomic batch is rejected before a byte of it is written. That second point is where "rejected before any command is sent" holds precisely.
- A keyless operation routes to the pinned connection; one issued before any key, and an all-keyless transaction, route to an arbitrary live master (`pickNode`). These are unusual patterns supported permissively rather than rejected.
- A transaction never follows a redirect or auto-retries: a `MOVED`/`ASK`, `NotConnected`, or `ConnectionLost` during the scope surfaces as-is and triggers a background topology refresh so the *next* attempt pins correctly — transparently re-pinning mid-scope would break `WATCH`/`MULTI` atomicity. The caller's existing retry loop (already needed for the `None` watch-abort) absorbs it.
- Acquisition is deferred, so the lease bracket acquires an unpinned scope and the release is a no-op when no key was ever touched; once pinned, the standalone lease hygiene (quiescence, discard-if-armed, force-close on `close`) runs unchanged against the pinning node's pool.
