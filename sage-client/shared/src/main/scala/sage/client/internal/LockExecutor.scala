package sage.client.internal

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.{DecodeError, InvalidArgument, LockLost, TimedOut}
import sage.client.BackoffConfig
import sage.codec.KeyCodec
import sage.commands.*

final private[client] class LockExecutor[K](
  leaseDuration: FiniteDuration,
  namespace: String,
  replicaAcknowledgement: Boolean
)(using codec: KeyCodec[K]) {
  import LockExecutor.Operation

  private val namespaced        = SingleKeyScript.namespaced(namespace)
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

  final private class Attempt(val runner: SharedRunner, val key: Bytes) {
    val token: String   = UUID.randomUUID().toString
    val stopped         = new AtomicBoolean(false)
    val mayOwn          = new AtomicBoolean(false)
    val renewedAt       = new AtomicLong(0L)
    val releaseDeadline = new AtomicReference[Deadline]()
  }

  private[internal] def key(value: K): Bytes = namespaced(codec.encode(value))

  private[internal] def command(key: Bytes, token: String, operation: Operation, cached: Boolean): Command[Boolean] =
    Command(
      LockExecutor.compiled.verb(cached),
      SingleKeyScript.KeyIndices,
      Vector(
        LockExecutor.compiled.reference(cached),
        SingleKeyScript.NumKeys,
        key,
        Bytes.utf8(token),
        Bytes.utf8(operation.wireName),
        Bytes.utf8(leaseDuration.toMillis.toString)
      ),
      _.asLong.flatMap {
        case 0 => Right(false)
        case 1 => Right(true)
        case n => Left(DecodeError("lock result 0 or 1", n.toString))
      }
    )

  def tryWithLock[A](runner: SharedRunner, key: K)(body: => CIO[A]): CIO[Option[A]] = {
    val bodyThunk = () => body
    validate(None).flatMap(_ => attempt(runner, this.key(key), None)(bodyThunk))
  }

  def withLock[A](runner: SharedRunner, key: K, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A] = {
    val bodyThunk = () => body
    validate(Some(waitTimeout)).flatMap { _ =>
      CIO.nowMonotonic.flatMap { started =>
        val encoded                  = this.key(key)
        val wait                     = Deadline(started.toNanos, waitTimeout.toNanos)
        def loop(retry: Int): CIO[A] = attempt(runner, encoded, Some(wait))(bodyThunk).flatMap {
          case Some(value) => CIO.value(value)
          case None        =>
            remainingWait(wait).flatMap { remaining =>
              retryAfter(remaining, contentionBackoff, retry)(loop)
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
    state: Attempt,
    operation: Operation,
    confirmationDeadline: Option[Deadline] = None
  ): CIO[Boolean] = {
    def run(cached: Boolean): CIO[Boolean] = {
      val command = this.command(state.key, state.token, operation, cached)
      confirmationDeadline match {
        case None           => state.runner.run(command)
        case Some(deadline) =>
          CIO.nowMonotonic.flatMap { now =>
            val remaining = math.min(operationTimeout.toNanos, deadline.remainingAt(now.toNanos))
            if (remaining <= 0L) CIO.fail(TimedOut("distributed lock write timed out"))
            else state.runner.lockWrite(command, remaining.nanos, replicaAcknowledgement)
          }
      }
    }
    Client.withScriptFallback(run)
  }

  private def attempt[A](runner: SharedRunner, key: Bytes, wait: Option[Deadline])(body: () => CIO[A]): CIO[Option[A]] =
    // Allocate only local state during acquisition so a contended or unresponsive server never masks cancellation of the wait loop.
    CIO.acquireReleaseWith(CIO.defer(new Attempt(runner, key)))(cleanup) { state =>
      val budget = wait.fold(CIO.value(operationTimeout.toNanos))(remainingWait)
      budget.flatMap { waitRemaining =>
        CIO.nowMonotonic.flatMap { started =>
          state.mayOwn.set(true)
          val deadline = Deadline(started.toNanos, waitRemaining)
          CIO
            .timeoutWithError(waitRemaining.nanos)(TimedOut("distributed lock acquisition timed out"))(
              acquire(state, deadline, retry = 0)
            )
            .flatMap {
              case false =>
                state.mayOwn.set(false)
                CIO.value(None)
              case true  =>
                val checkWait = wait.fold(CIO.unit)(remainingWait(_).unit)
                checkWait.flatMap(_ => runWithAcquiredLock(state)(body)).map(Some(_))
            }
        }
      }
    }

  private def runWithAcquiredLock[A](state: Attempt)(body: () => CIO[A]): CIO[A] =
    remainingLease(state).flatMap { _ =>
      val work = CIO.defer(()).flatMap(_ => body()).flatMap(value => remainingLease(state).map(_ => value))
      // Returning errors as values lets the race stop on either body failure or lock loss.
      CIO.race(work.liftToTry, renew[A](state).liftToTry).flatMap(CIO.get(_)).flatMap { value =>
        release(state).map(_ => value)
      }
    }

  private def release(state: Attempt): CIO[Unit] = {
    state.stopped.set(true)
    remainingLease(state).flatMap { remaining =>
      remainingRelease(state).flatMap { releaseRemaining =>
        CIO
          .timeoutWithError(math.min(remaining, releaseRemaining).nanos)(LockLost("distributed lock release timed out"))(
            eval(state, Operation.Release)
          )
          .flatMap { released =>
            state.mayOwn.set(false)
            if (released) CIO.unit
            else CIO.fail(LockLost("distributed lock ownership was lost before release"))
          }
      }
    }
  }

  private def acquire(
    state: Attempt,
    deadline: Deadline,
    retry: Int
  ): CIO[Boolean] =
    CIO.nowMonotonic.flatMap { started =>
      state.renewedAt.set(started.toNanos)
      eval(state, Operation.Acquire, Some(deadline)).recover {
        case error if retryableFailure(error) =>
          CIO.nowMonotonic.flatMap { now =>
            val remaining = deadline.remainingAt(now.toNanos)
            if (remaining <= 0L) CIO.fail(TimedOut("distributed lock acquisition timed out"))
            else retryAfter(remaining, failureBackoff, retry)(acquire(state, deadline, _))
          }
        case error                            => CIO.fail(error)
      }
    }

  private def renew[A](state: Attempt): CIO[A] =
    remainingLease(state).flatMap { beforeDelay =>
      // Acquisition confirmation can leave less than the usual renewal delay. Wake by the midpoint of the remaining conservative lease.
      val delay = math.min(renewalDelay.toNanos, math.max(1L, beforeDelay / 2L)).nanos
      CIO.sleep(delay).flatMap { _ =>
        if (state.stopped.get()) CIO.never
        else
          renewAttempt(state, retry = 0).flatMap(_ => renew[A](state))
      }
    }

  private def renewAttempt(state: Attempt, retry: Int): CIO[Unit] =
    remainingLease(state).flatMap { remaining =>
      CIO.nowMonotonic.flatMap { started =>
        CIO
          .timeoutWithError(remaining.nanos)(LockLost("distributed lock renewal timed out"))(
            eval(state, Operation.Renew, Some(Deadline(started.toNanos, remaining)))
          )
          .flatMap { renewed =>
            if (!renewed) CIO.fail(LockLost("distributed lock ownership was lost during renewal"))
            else
              remainingLease(state).map { _ =>
                state.renewedAt.set(started.toNanos)
              }
          }
          .recover {
            case error if retryableFailure(error) => retryRenewal(state, retry, error)
            case error                            => renewalFailed(error)
          }
      }
    }

  private def retryRenewal(
    state: Attempt,
    retry: Int,
    lastError: Throwable
  ): CIO[Unit] =
    retryRemainingLease(state, lastError).flatMap { remaining =>
      retryAfter(remaining, failureBackoff, retry) { nextRetry =>
        CIO.defer(()).flatMap { _ =>
          if (state.stopped.get()) CIO.never
          else retryRemainingLease(state, lastError).flatMap(_ => renewAttempt(state, nextRetry))
        }
      }
    }

  private def retryRemainingLease(state: Attempt, lastError: Throwable): CIO[Long] =
    remainingLease(state).recover { case _ => renewalFailed(lastError) }

  private def retryAfter[A](remainingNanos: Long, config: BackoffConfig, retry: Int)(next: Int => CIO[A]): CIO[A] = {
    val delay = math.min(remainingNanos, Scheduler.real.backoffMillis(config, retry).millis.toNanos)
    CIO.sleep(delay.nanos).flatMap(_ => next(if (retry < Int.MaxValue) retry + 1 else retry))
  }

  private def retryableFailure(error: Throwable): Boolean =
    if (error.isInstanceOf[TimedOut]) !LockReplication.isAcknowledgementFailure(error)
    else
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

  private def cleanup(state: Attempt): CIO[Unit] = CIO.defer(()).flatMap { _ =>
    // Future/Pekko cannot cancel the losing race branch. This flag also stops their renewal loop after the scope has finished.
    state.stopped.set(true)
    if (!state.mayOwn.get()) CIO.unit
    else
      remainingRelease(state).flatMap { remaining =>
        if (remaining <= 0) CIO.unit
        else CIO.timeout(remaining.nanos)(eval(state, Operation.Release)).unit.recover(_ => CIO.unit)
      }
  }
}

private[client] object LockExecutor {
  enum Operation(val wireName: String) {
    case Acquire extends Operation("acquire")
    case Renew   extends Operation("renew")
    case Release extends Operation("release")
  }

  val script: String =
    """local token = ARGV[1]
      |local operation = ARGV[2]
      |if operation == 'acquire' then
      |  if redis.call('SET', KEYS[1], token, 'NX', 'PX', ARGV[3]) then return 1 end
      |  if redis.call('GET', KEYS[1]) == token then return redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
      |  return 0
      |end
      |if operation ~= 'renew' and operation ~= 'release' then
      |  return redis.error_reply('SAGE invalid lock operation')
      |end
      |if redis.call('GET', KEYS[1]) ~= token then return 0 end
      |if operation == 'renew' then return redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
      |return redis.call('DEL', KEYS[1])
      |""".stripMargin

  val compiled = SingleKeyScript(script)
}
