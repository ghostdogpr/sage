# Per-position Stream Entry ID types

A Stream Entry ID appears in many command positions, and each position admits a different set of special tokens beyond a concrete `ms-seq` value: `XADD` takes `*` (auto) or `<ms>-*` (auto-seq); `XRANGE`/`XREVRANGE`/`XPENDING`/`XAUTOCLAIM` bounds take `-`/`+` extremes and the `(` exclusive prefix; `XREAD` takes `$` (last id) or `+` (last entry); `XREADGROUP` takes `>` (new-for-group); `XGROUP CREATE`/`XSETID` take `$`. Sage models each position as its own sealed type (`XAddId`, `StreamRangeId`, `ReadId`, `GroupReadId`, `GroupStartId`), each listing only the tokens legal there, with the concrete `StreamId(ms, seq)` as the shared common case and the sole type used for replies and explicit writes. An illegal form at a position — `>` to `XADD`, `(` to `XREAD` — therefore cannot be written.

No mainstream client does this. Jedis exposes a single `StreamEntryID` type carrying command-scoped sentinel *constants* (`NEW_ENTRY`, `MINIMUM_ID`, `XREADGROUP_UNDELIVERED_ENTRY`, …) — all the same type, so the compiler cannot stop a token reaching the wrong command. Lettuce uses position-specific *factories* (`StreamOffset.latest()`, `Range.Boundary.excluding()`) but the carried value is a bare `String`. go-redis, redis-py, and zio-redis pass raw strings (or a generic), validated only by the server.

The cost is real — five ID types instead of one, and more ceremony at the call site than `"*"` or `">"`. We accept it because it is the same trade sage already made for `ZRange`/`ScoreBoundary`/`GeoOrigin` (ADR-0011, ADR-0018, ADR-0030): the whole library's thesis is that illegal states are unrepresentable rather than rejected at runtime, and a stringly-typed or single-sentinel ID reintroduces exactly the runtime rejection the no-throwing-in-builders rule forbids. This is hard to reverse — the entire family's signatures hang off these types — which is why it is recorded.

## Considered Options

- **Per-position sealed types (chosen)** — illegal token-at-position unrepresentable; consistent with the existing range/boundary modeling. Costs more types and call-site verbosity.
- **One `StreamId` + sentinel constants (Jedis)** — fewer types, familiar, but nothing prevents `>` reaching `XADD`; the server rejects it at runtime, against the unrepresentable-illegal-states stance.
- **One `StreamId` + a shared token enum, validated at encode** — fewest types, but reintroduces runtime rejection inside a builder, which the no-throwing rule forbids.
