package kyo.compat

import java.util.concurrent.CompletionStage

import cats.effect.IO

/**
  * Lifts a `CompletionStage[A]` into `CIO[A]` via `IO.fromCompletionStage`; this integration is JVM-only. Failures propagate through the
  * `IO` error channel, and `CIO` interruption propagates to the source via `cf.cancel(true)`.
  */
object CompatFromCompletionStage {

  /**
    * Lifts `cs` into a `CIO[A]` that observes its eventual completion; `CIO` interruption propagates to `cs` via `cf.cancel(true)`.
    */
  inline def fromCompletionStage[A](inline cs: CompletionStage[A]): CIO[A] =
    CIO.lift(IO.fromCompletionStage(IO.delay(cs)))

}

extension (inline c: CIO.type) {

  /**
    * Lifts `cs` into a `CIO[A]` that observes its eventual completion; `CIO` interruption propagates to `cs` via `cf.cancel(true)`.
    */
  inline def fromCompletionStage[A](inline cs: CompletionStage[A]): CIO[A] =
    CompatFromCompletionStage.fromCompletionStage(cs)
}
