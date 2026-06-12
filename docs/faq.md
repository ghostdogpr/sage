# FAQ

<!--
WRITER NOTES — replace this body with real content.
Format: a list of `## Question?` headings, each with a short answer. Anchors are auto-
generated so nav/cross-links can deep-link (see how proteus/purelogic FAQ pages read).

Candidate questions (pick the ones worth answering; add others as they come up):

- Why a native protocol implementation instead of wrapping an existing Java client?
    Differentiator + control over allocation/RESP3; tie to "Native Redis protocol".
    (Don't name specific Java clients in the rendered docs.)
- Which backend artifact should I use?
    ZIO → sage-client-zio, cats-effect → sage-client-ce, Kyo → sage-client-kyo,
    Ox → sage-client-ox. They share the same runtime; pick your effect system.
- Does every command borrow a connection from a pool?
    No — ordinary commands are auto-pipelined on the Multiplexed Connection; only
    WATCH/MULTI/EXEC and blocking commands take a Dedicated Connection. (See the
    "Example dialogue" in CONTEXT.md.)
- Redis or Valkey? Which versions?
    RESP3, Redis 8+ / Valkey 8+.
- Scala / JDK requirements?
    Scala 3.3.x LTS and later; JDK 21+ (blocking I/O on virtual threads).
- Is Scala.js / Scala Native supported?
    Core is JVM-only (ADR 0041) — state the fact plainly, do not cite the ADR.
- Sentinel support?
    Out of scope (per the Master-Replica glossary entry).

State rationale in plain words — never reference ADRs / issues / the PRD in rendered docs.
-->
