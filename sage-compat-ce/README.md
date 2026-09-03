# sage-compat-ce

This module implements the full [kyo-compat](https://github.com/getkyo/kyo/tree/main/kyo-compat) API with `cats.effect.IO` and fs2. Sage defines its runtime against `kyo.compat`. The `-ce` cells of `sage-client` and downstream modules compile and run against this implementation.

## Provenance

Kyo removed its Cats Effect integrations in 1.0.0-RC6 in [kyo#1779](https://github.com/getkyo/kyo/pull/1779) and moved community bindings outside the project in [kyo#1840](https://github.com/getkyo/kyo/pull/1840). This module vendors the last upstream Cats Effect binding from commit [`eae31e1d`](https://github.com/getkyo/kyo/tree/eae31e1d39d4b8ff2df168272e60b38d9e9dd502/kyo-compat/bindings/ce). Those sources match `io.getkyo:kyo-compat-ce_3:1.0.0-RC5`.

The vendored copy merges the `shared` and `jvm` source trees because Sage supports only the JVM. It also uses this repository's brace-based formatting and corrects several comments.

Sage changes `CIO.async` from `IO.async_` to `IO.async` with a cancellation token so a timeout or cancellation can stop waiting for a callback. This applies to every CE command wait, including distributed lock acquisition, renewal, and release. Cancellation does not stop the external operation or retract a command already sent to the server. The transport still consumes its reply in order. The conformance suite checks compatibility with the shared API, and lock regression tests cover delayed callbacks.

Kyo is licensed under [Apache 2.0](https://github.com/getkyo/kyo/blob/main/LICENSE), the same license as Sage.

## Keeping it in sync

The binding depends only on Cats Effect and fs2, so a Kyo upgrade does not change this module directly. A Kyo release can still add operations to `kyo.compat` or change their behavior. The cross-binding conformance suite detects those changes. `kyo-compat-plugin` contains the suite, and the `ceConformance` matrix runs it against this module. CI includes it in `testUnit`. Run `sbt conformanceCe` to execute only this suite.

When the suite finds a difference, port the matching update from another binding such as [bindings/future](https://github.com/getkyo/kyo/tree/main/kyo-compat/bindings/future) and translate its operations to Cats Effect.
