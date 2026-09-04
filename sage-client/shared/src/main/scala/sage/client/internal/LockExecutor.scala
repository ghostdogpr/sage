package sage.client.internal

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.{InvalidArgument, LockLost, ServerError, TimedOut}
import sage.client.BackoffConfig
import sage.codec.KeyCodec

final private[client] class LockExecutor[K](
  leaseDuration: FiniteDuration,
  namespace: String,
  replicaAcknowledgement: Boolean = true
)(using KeyCodec[K]) {
  import LockCommands.Operation

  private val commands          = new LockCommands[K](leaseDuration, namespace)
  // The usable lease accounts for millisecond rounding, scheduling, clock drift, and time spent waiting for replies.
  private val leaseNanos        = leaseDuration.toMillis * 1000000L
  private val usableNanos       = leaseNanos - leaseNanos / 10L
  private val renewalDelay      = (leaseNanos / 3L).nanos
  private val operationTimeout  = math.min(usableNanos, 1.second.toNanos).nanos
  private val contentionBackoff = BackoffConfig(initialDelay = 50.millis, maxDelay = 500.millis, multiplier = 2.0)
  private val failureBackoff    = BackoffConfig(initialDelay = 10.millis, maxDelay = 100.millis, multiplier = 2.0)

  final private case class Deadline(startedAtNanos: Long, durationNanos: Long) {
    def remainingAt(nowNanos: Long): Long = durationNanos - (nowNanos - startedAtNanos)
  }

  final private class Attempt(val key: Bytes) {
    val token: String   = UUID.randomUUID().toString
    val stopped         = new AtomicBoolean(false)
    val mayOwn          = new AtomicBoolean(false)
    val renewedAt       = new AtomicLong(0L)
    val releaseDeadline = new AtomicReference[Deadline]()
  }

  def tryWithLock[A](runner: CommandRunner[CIO, String], key: K)(body: => CIO[A]): CIO[Option[A]] = {
    val bodyThunk = () => body
    validate(None).flatMap(_ => attempt(runner, commands.key(key), None)(bodyThunk))
  }

  def withLock[A](runner: CommandRunner[CIO, String], key: K, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A] = {
    val bodyThunk = () => body
    validate(Some(waitTimeout)).flatMap { _ =>
      CIO.nowMonotonic.flatMap { started =>
        val encoded                  = commands.key(key)
        val wait                     = Deadline(started.toNanos, waitTimeout.toNanos)
        def loop(retry: Int): CIO[A] = attempt(runner, encoded, Some(wait))(bodyThunk).flatMap {
          case Some(value) => CIO.value(value)
          case None        =>
            remainingWait(wait).flatMap { remaining =>
              val delay = math.min(remaining, Backoff.jitteredMillis(contentionBackoff, retry, Scheduler.real).millis.toNanos)
              CIO.sleep(delay.nanos).flatMap(_ => loop(if (retry < Int.MaxValue) retry + 1 else retry))
            }
        }
        loop(0)
      }
    }

  }

  private def validate(wait: Option[FiniteDuration]): CIO[Unit] =
    if (leaseDuration < 30.millis) CIO.fail(InvalidArgument("leaseDuration must be at least 30 milliseconds"))
    else if (wait.exists(_ <= Duration.Zero)) CIO.fail(InvalidArgument("waitTimeout must be positive"))
    else CIO.unit

  private def remainingWait(wait: Deadline): CIO[Long] = CIO.nowMonotonic.flatMap { now =>
    val remaining = wait.remainingAt(now.toNanos)
    if (remaining <= 0) CIO.fail(TimedOut("distributed lock acquisition timed out"))
    else CIO.value(remaining)
  }

  private def remainingLease(state: Attempt): CIO[Long] = CIO.nowMonotonic.flatMap { now =>
    val remaining = usableNanos - (now.toNanos - state.renewedAt.get())
    if (remaining <= 0) CIO.fail(LockLost("distributed lock lease expired before ownership could be confirmed"))
    else CIO.value(remaining)
  }

  private def remainingRelease(state: Attempt): CIO[Long] = CIO.nowMonotonic.map { now =>
    val deadline = state.releaseDeadline.updateAndGet { current =>
      if (current == null) Deadline(now.toNanos, operationTimeout.toNanos) else current
    }
    deadline.remainingAt(now.toNanos)
  }

  private def eval(
    runner: CommandRunner[CIO, String],
    state: Attempt,
    operation: Operation,
    confirmationDeadline: Option[Deadline] = None
  ): CIO[Boolean] = {
    def run(cached: Boolean): CIO[Boolean] = {
      val command = commands.command(state.key, state.token, operation, cached)
      confirmationDeadline match {
        case None           => runner.run(command)
        case Some(deadline) =>
          CIO.nowMonotonic.flatMap { now =>
            val remaining = math.min(operationTimeout.toNanos, deadline.remainingAt(now.toNanos))
            if (remaining <= 0L) CIO.fail(TimedOut("distributed lock write timed out"))
            else runner.lockWrite(command, remaining.nanos, replicaAcknowledgement)
          }
      }
    }
    run(cached = true).recover {
      case ServerError("NOSCRIPT", _) => run(cached = false)
      case other                      => CIO.fail(other)
    }
  }

  private def attempt[A](runner: CommandRunner[CIO, String], key: Bytes, wait: Option[Deadline])(body: () => CIO[A]): CIO[Option[A]] =
    // Allocate only local state during acquisition so a contended or unresponsive server never masks cancellation of the wait loop.
    CIO.acquireReleaseWith(CIO.defer(new Attempt(key)))(cleanup(runner, _)) { state =>
      val budget = wait.fold(CIO.value(operationTimeout.toNanos))(remainingWait)
      budget.flatMap { waitRemaining =>
        CIO.nowMonotonic.flatMap { started =>
          state.renewedAt.set(started.toNanos)
          state.mayOwn.set(true)
          val duration = math.min(waitRemaining, usableNanos)
          val deadline = Deadline(started.toNanos, duration)
          CIO
            .timeoutWithError(duration.nanos)(TimedOut("distributed lock acquisition timed out"))(
              acquire(runner, state, deadline, retry = 0)
            )
            .flatMap {
              case false =>
                state.mayOwn.set(false)
                CIO.value(None)
              case true  =>
                val checkWait = wait.fold(CIO.unit)(remainingWait(_).unit)
                checkWait.flatMap(_ => remainingLease(state)).flatMap { _ =>
                  val work = CIO.defer(()).flatMap(_ => body()).flatMap(value => remainingLease(state).map(_ => value))
                  // The renewal branch starts before the lease midpoint and bounds its write by the remaining lease, so it also watches the
                  // body deadline. Returning errors as values lets the race stop on either body failure or lock loss.
                  CIO.race(work.liftToTry, renew[A](runner, state).liftToTry).flatMap(CIO.get(_)).flatMap { value =>
                    state.stopped.set(true)
                    remainingLease(state).flatMap { remaining =>
                      remainingRelease(state).flatMap { releaseRemaining =>
                        CIO
                          .timeoutWithError(math.min(remaining, releaseRemaining).nanos)(LockLost("distributed lock release timed out"))(
                            eval(runner, state, Operation.Release)
                          )
                          .flatMap { released =>
                            state.mayOwn.set(false)
                            if (released) CIO.value(Some(value))
                            else CIO.fail(LockLost("distributed lock ownership was lost before release"))
                          }
                      }
                    }
                  }
                }
            }
        }
      }
    }

  private def acquire(
    runner: CommandRunner[CIO, String],
    state: Attempt,
    deadline: Deadline,
    retry: Int
  ): CIO[Boolean] =
    eval(runner, state, Operation.Acquire, Some(deadline)).recover {
      case error if retryableAcquisitionFailure(error) =>
        CIO.nowMonotonic.flatMap { now =>
          val remaining = deadline.remainingAt(now.toNanos)
          if (remaining <= 0L) CIO.fail(TimedOut("distributed lock acquisition timed out"))
          else {
            val delay = math.min(remaining, Backoff.jitteredMillis(failureBackoff, retry, Scheduler.real).millis.toNanos)
            CIO.sleep(delay.nanos).flatMap(_ => acquire(runner, state, deadline, if (retry < Int.MaxValue) retry + 1 else retry))
          }
        }
      case error                                       => CIO.fail(error)
    }

  private def renew[A](runner: CommandRunner[CIO, String], state: Attempt): CIO[A] =
    remainingLease(state).flatMap { beforeDelay =>
      // Acquisition confirmation can leave less than the usual renewal delay. Wake by the midpoint of the remaining conservative lease.
      val delay = math.min(renewalDelay.toNanos, math.max(1L, beforeDelay / 2L)).nanos
      CIO.sleep(delay).flatMap { _ =>
        if (state.stopped.get()) CIO.never
        else
          renewAttempt(runner, state, retry = 0).flatMap(_ => renew[A](runner, state))
      }
    }

  private def renewAttempt(runner: CommandRunner[CIO, String], state: Attempt, retry: Int): CIO[Unit] =
    remainingLease(state).flatMap { remaining =>
      CIO.nowMonotonic.flatMap { started =>
        CIO
          .timeoutWithError(remaining.nanos)(LockLost("distributed lock renewal timed out"))(
            eval(runner, state, Operation.Renew, Some(Deadline(started.toNanos, remaining)))
          )
          .flatMap { renewed =>
            if (!renewed) CIO.fail(LockLost("distributed lock ownership was lost during renewal"))
            else
              remainingLease(state).map { _ =>
                state.renewedAt.set(started.toNanos)
              }
          }
          .recover {
            case error if retryableLockFailure(error) => retryRenewal(runner, state, retry, error)
            case error                                => renewalFailed(error)
          }
      }
    }

  private def retryRenewal(
    runner: CommandRunner[CIO, String],
    state: Attempt,
    retry: Int,
    lastError: Throwable
  ): CIO[Unit] =
    CIO.nowMonotonic.flatMap { now =>
      val remaining = usableNanos - (now.toNanos - state.renewedAt.get())
      if (remaining <= 0L) renewalFailed(lastError)
      else {
        val delay = math.min(remaining, Backoff.jitteredMillis(failureBackoff, retry, Scheduler.real).millis.toNanos)
        CIO.sleep(delay.nanos).flatMap { _ =>
          if (state.stopped.get()) CIO.never
          else
            CIO.nowMonotonic.flatMap { afterDelay =>
              if (usableNanos - (afterDelay.toNanos - state.renewedAt.get()) <= 0L) renewalFailed(lastError)
              else renewAttempt(runner, state, if (retry < Int.MaxValue) retry + 1 else retry)
            }
        }
      }
    }

  private def retryableLockFailure(error: Throwable): Boolean =
    error.isInstanceOf[TimedOut] || (Fault.categorize(error) match {
      case Fault.Redirected(_) | Fault.Demoted | Fault.Lost(_) | Fault.TryAgain | Fault.Unavailable(_) => true
      case Fault.Fatal                                                                                 => false
    })

  private def retryableAcquisitionFailure(error: Throwable): Boolean =
    Fault.categorize(error) match {
      case Fault.Redirected(_) | Fault.Demoted | Fault.Lost(_) | Fault.TryAgain | Fault.Unavailable(_) => true
      case Fault.Fatal                                                                                 => false
    }

  private def renewalFailed(cause: Throwable): CIO[Nothing] = cause match {
    case lost: LockLost => CIO.fail(lost)
    case other          =>
      val lost = LockLost("distributed lock renewal failed")
      lost.initCause(other)
      CIO.fail(lost)
  }

  private def cleanup(runner: CommandRunner[CIO, String], state: Attempt): CIO[Unit] = CIO.defer(()).flatMap { _ =>
    // Future/Pekko cannot cancel the losing race branch. This flag also stops their renewal loop after the scope has finished.
    state.stopped.set(true)
    if (!state.mayOwn.get()) CIO.unit
    else
      remainingRelease(state).flatMap { remaining =>
        if (remaining <= 0) CIO.unit
        else CIO.timeout(remaining.nanos)(eval(runner, state, Operation.Release)).unit.recover(_ => CIO.unit)
      }
  }
}
