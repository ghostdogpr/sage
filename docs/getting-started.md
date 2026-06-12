# Getting started

<!--
WRITER NOTES — replace this body with real content.

This is the single entry point (install + first usage + a short tour), like the
proteus/purelogic "Getting started" pages. There is no separate Quickstart page.
Keep prose tight; link out to the feature guides at the end for depth.

Sections to cover:

1. Intro paragraph (2–4 sentences). Pull positioning from README.md:
   - native Redis/Valkey client for Scala 3, no Java client wrapped underneath
   - pure sans-IO core + runtime cross-published per backend
   - usable from ZIO, cats-effect, Kyo, Ox with native types
   - targets RESP3, Redis 8+ / Valkey 8+; Scala 3.3.x LTS+; JDK 21+

2. Installation — Maven coordinates, ONE per backend, in a code-group with a tab per
   backend. Use the @VERSION@ placeholder (substituted at deploy by docs.yml).
   Coordinates (confirmed against build.sbt):
     "com.github.ghostdogpr" %% "sage-client-zio" % "@VERSION@"
     "com.github.ghostdogpr" %% "sage-client-ce"  % "@VERSION@"
     "com.github.ghostdogpr" %% "sage-client-kyo" % "@VERSION@"
     "com.github.ghostdogpr" %% "sage-client-ox"  % "@VERSION@"
   Note: sage-core is transitive; users depend on the backend artifact only. JDK 21+.

3. Two imports cover everything (from examples/README.md):
     import sage.*            // command vocabulary + connection config
     import sage.<backend>.*  // the client

4. Connect — code-group with a tab per backend (this is the one genuinely per-backend
   bit): ZIO `layer`, cats-effect `resource`, Kyo `scoped`, Ox `scoped`. Sources:
     examples/zio/.../Tour.scala, examples/ce/.../Tour.scala,
     examples/kyo/.../Tour.scala, examples/ox/.../Tour.scala

5. "How it works" aside (callout box) — the CONNECTION MODEL lives here (it is NOT a
   separate page, and auto-pipelining is NOT a heading next to "Pipelines"). Explain,
   grounded in CONTEXT.md glossary:
     - Multiplexed Connection — ordinary commands from all fibers share one
       auto-pipelined connection; replies matched FIFO.
     - Auto-pipelining — a transparent PROPERTY of the Multiplexed Connection
       (invisible to the user), distinct from a Pipeline (a value you build).
     - Dedicated Connection — acquired transparently for WATCH/MULTI/EXEC and
       blocking commands.
     - Subscription Connection — lazily created, isolates slow pub/sub consumers.
   The "Example dialogue" at the bottom of CONTEXT.md is good raw material.

6. A short tour, shown ONCE in Ox direct style (no effect-wrapper noise; do NOT repeat
   every snippet four times). Reuse the example files, don't invent:
     - Run a command across a couple families — CommandsExample.scala (+ shared/Domain.scala)
     - Pipeline — PipelinesExample.scala
     - Transaction — TransactionsExample.scala
     - Pub/Sub — PubSubExample.scala
     - Cached read — CachedReadsExample.scala

7. Close with links to the feature guides (Commands & codecs, Pipelines & transactions,
   Pub/Sub, Client-side caching, Deployments).
-->
