package sage.client.internal

import kyo.compat.*

import sage.SageException.ServerError
import sage.commands.Command
import sage.ratelimit.{Decision, RateLimiter}

/**
  * Runs one bound limiter. [[eval]] sends the whole script each call; [[evalSha]] validates, runs by digest, and reloads once on a `NOSCRIPT`.
  */
final private[client] class RateLimitExecutor[K](definition: RateLimiter[K]) {

  def eval[F[_]](runner: CommandRunner[F, ?], subject: K, cost: Long): F[Decision] =
    runner.run(definition.tryAcquire(subject, cost))

  def command(subject: K, cost: Long): Command[Decision] = definition.tryAcquire(subject, cost)

  def resetCommand(subject: K): Command[Unit] = definition.reset(subject)

  def evalSha(runner: CommandRunner[CIO, String], subject: K, cost: Long): CIO[Decision] =
    definition.validate(cost) match {
      case Some(problem) => CIO.fail(new IllegalArgumentException(s"invalid rate limiter: $problem"))
      case None          =>
        val check = definition.evalSha(subject, cost)
        runner.run(check).recover {
          case ServerError(code, _) if code == "NOSCRIPT" => runner.run(definition.loadCommand).flatMap(_ => runner.run(check))
          case other                                      => CIO.fail(other)
        }
    }
}
