package sage.client.internal

import kyo.compat.*

import sage.SageException.{InvalidArgument, ServerError}
import sage.commands.Command
import sage.ratelimit.{Decision, RateLimiter}

/**
  * Runs one bound limiter: [[evalSha]] validates, runs by digest, and reloads the script once on a `NOSCRIPT`.
  */
final private[client] class RateLimitExecutor[K](definition: RateLimiter[K]) {

  def command(subject: K, cost: Long): Command[Decision] = definition.tryAcquire(subject, cost)

  def resetCommand(subject: K): Command[Unit] = definition.reset(subject)

  def evalSha(runner: CommandRunner[CIO, String], subject: K, cost: Long, peek: Boolean): CIO[Decision] =
    definition.validate(cost) match {
      case Some(problem) => CIO.fail(InvalidArgument(problem))
      case None          =>
        val check = definition.evalSha(subject, cost, peek)
        runner.run(check).recover {
          case ServerError(code, _) if code == "NOSCRIPT" => runner.run(definition.loadCommand).flatMap(_ => runner.run(check))
          case other                                      => CIO.fail(other)
        }
    }
}
