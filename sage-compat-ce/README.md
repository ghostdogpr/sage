# sage-compat-ce

This module is the Cats Effect binding of the [kyo-compat](https://github.com/getkyo/kyo/tree/main/kyo-compat) surface: it implements
the full `kyo.compat` package on `cats.effect.IO` and fs2. Sage's runtime is written once against that surface, and the `-ce` cells of
`sage-client` and every downstream module compile and run against this binding.

## Provenance

Kyo removed its Cats Effect integrations in 1.0.0-RC6 ([kyo#1779](https://github.com/getkyo/kyo/pull/1779)) and invited the community to
maintain them externally ([kyo#1840](https://github.com/getkyo/kyo/pull/1840)). This module vendors the binding from its last upstream
state, commit [`eae31e1d`](https://github.com/getkyo/kyo/tree/eae31e1d39d4b8ff2df168272e60b38d9e9dd502/kyo-compat/bindings/ce), whose
sources match the published `io.getkyo:kyo-compat-ce_3:1.0.0-RC5` exactly. The vendored copy differs textually from that state: the
`shared` and `jvm` source trees are merged because Sage is JVM-only, the code is reformatted to this repository's brace-based style, and
several pieces of documentation are corrected or completed. The conformance suite described below verifies that it stays equivalent to
upstream in API and behavior.

Kyo is licensed under [Apache 2.0](https://github.com/getkyo/kyo/blob/main/LICENSE), the same license as Sage.

## Keeping it in sync

The binding's only dependencies are Cats Effect and fs2, so upgrading Kyo does not affect this module directly. What can change is the
`kyo.compat` contract itself: a Kyo release may add operations to the surface or adjust the semantics the bindings must implement. The
cross-binding conformance suite catches that. The suite is bundled inside `kyo-compat-plugin`, the `ceConformance` matrix compiles and
runs it against this module, and CI runs it as part of `testUnit` (`sbt conformanceCe` runs it alone). A failure there identifies which
operations are missing or which semantics changed; port the corresponding update from another binding, for example
[bindings/future](https://github.com/getkyo/kyo/tree/main/kyo-compat/bindings/future), translating the primitives to Cats Effect.
