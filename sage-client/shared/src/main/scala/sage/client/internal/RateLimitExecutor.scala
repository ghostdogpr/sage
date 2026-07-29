package sage.client.internal

import kyo.compat.*

import sage.SageException.{InvalidArgument, ServerError}
import sage.commands.Command
import sage.ratelimit.{Decision, RateLimiter}

/**
  * Runs one bound limiter: [[evalSha]] validates, runs by digest, and falls back once to sending the script body on a `NOSCRIPT`. The
  * fallback is keyed like the check itself, so in a cluster it routes to the one owning master and needs no all-masters `SCRIPT LOAD`.
  */
final private[client] class RateLimitExecutor[K](definition: RateLimiter[K]) {

  def command(subject: K, cost: Long): Command[Decision] = definition.tryAcquire(subject, cost)

  def resetCommand(subject: K): Command[Unit] = definition.reset(subject)

  def evalSha(runner: CommandRunner[CIO, String], subject: K, cost: Long, peek: Boolean): CIO[Decision] =
    definition.validate(cost) match {
      case Some(problem) => CIO.fail(InvalidArgument(problem))
      case None          =>
        runner.run(definition.evalSha(subject, cost, peek)).recover {
          case ServerError(code, _) if code == "NOSCRIPT" => runner.run(definition.evalScript(subject, cost, peek))
          case other                                      => CIO.fail(other)
        }
    }
}
