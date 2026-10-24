# sage-compat-direct

This module implements the [kyo-compat](https://github.com/getkyo/kyo/tree/main/kyo-compat) API for plain blocking code. It depends only on the Scala library and JDK 21. The `-direct` cells of `sage-client` and downstream modules compile and run against it, so `sage-client-direct` returns results directly instead of wrapping them in an effect type.

## How it works

A `CIO[A]` is a function from the current `flatMap` nesting depth to the result, the same carrier as Kyo's Ox binding without the Ox capability. Running it with `lower` calls the function at depth 0 on the calling thread. `flatMap` passes the depth on and calls its functions directly. Once the depth reaches 256, the rest of the chain runs on a fresh virtual thread that the caller waits for, so long `flatMap` chains and recursive loops do not overflow the stack. That thread inherits the caller's `CLocal` values, its failures reach the caller unchanged, and interrupting the caller interrupts it and waits for it to exit. Failures are thrown exceptions and cancellation is thread interruption.

`CIO.async` blocks the calling thread until the callback fires. This is cheap on a virtual thread, which is how the binding is meant to be used.

Operations that need concurrency start JDK virtual threads and wait for every thread they start before they return:

- `timeout` runs the computation on a virtual thread and interrupts it at the deadline.
- `race` runs its first argument on the calling thread and its second on a virtual thread. Sage's distributed locks pass the user's body as the first argument, so the body keeps the caller's thread-local context. This also holds when a long `flatMap` chain has already moved to a fresh virtual thread: the thread that called `lower` waits for that chain and runs the race when the chain reaches it. If the second argument wins, the caller is interrupted to stop the first, and that interrupt is cleared before `race` returns.
- `acquireReleaseWith` runs `release` on a separate virtual thread and waits for it without reacting to interrupts, so a pending interrupt cannot cut the release short.
- `zip`, `foreach`, and the other collection operations run each element on its own virtual thread.

`CFiber.init` starts a virtual thread that is not tied to the computation that started it. `CLocal` uses `InheritableThreadLocal`, so virtual threads started by the binding see the values that were current when they started.

The atomic types, `CPromise`, `CChannel`, `CLatch`, `CMeter`, and `CChunk` use the same JDK and Scala types as Kyo's Ox binding.

## Conformance

`kyo-compat-plugin` contains a conformance suite shared by every binding. The `directConformance` matrix runs it against this module, and CI includes it in `testUnit`. Run `sbt conformanceDirect` to execute only this suite.
