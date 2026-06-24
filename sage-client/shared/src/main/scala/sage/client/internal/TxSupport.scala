package sage.client.internal

import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}

import scala.util.{Failure, Success, Try}

import kyo.compat.*

import sage.SageException
import sage.SageException.{DecodeError, ProtocolError, TransactionDiscarded}
import sage.commands.{Command, Reply}
import sage.protocol.Frame

/**
  * The Pipeline/Transaction result-shaping shared by the standalone client and the cluster runtime, kept in one place so the
  * per-position model and the MULTI/EXEC interpretation cannot drift between them.
  */
private[internal] object TxSupport {

  def collapseStrict[Out](results: Vector[Either[SageException, Any]], toOut: Vector[Any] => Out): CIO[Out] =
    results.collectFirst { case Left(error) => error } match {
      case Some(error) => CIO.fail(error)
      case None        => CIO.value(toOut(results.collect { case Right(value) => value }))
    }

  // decoders return Either and never throw; a non-SageException here is a decoder bug surfaced as a decode failure rather than allowed
  // to escape the per-position result model.
  def toEither(result: Try[Any]): Either[SageException, Any] =
    result match {
      case Success(value)            => Right(value)
      case Failure(e: SageException) => Left(e)
      case Failure(other)            => Left(DecodeError.fromThrowable(other))
    }

  // None = WATCH abort; Some = the per-position decoded results. A queueing-phase error fails the effect (nothing ran).
  def interpretExec(commands: Vector[Command[?]], frames: Vector[Frame]): CIO[Option[Vector[Either[SageException, Any]]]] = {
    val n          = commands.length
    // frames: MULTI reply, then one queue reply per command, then the EXEC reply
    val queueError = (0 to n).iterator.map(i => errorOf(frames(i))).collectFirst { case Some(message) => message }
    queueError match {
      case Some(message) => CIO.fail(TransactionDiscarded(message))
      case None          =>
        frames(n + 1) match {
          case Frame.Null                              => CIO.value(None)
          case Frame.Array(elems) if elems.length == n =>
            CIO.value(Some(Vector.tabulate(n)(i => toEither(Reply.decode(commands(i), elems(i))))))
          case Frame.Array(elems)                      =>
            CIO.fail(ProtocolError(s"EXEC returned ${elems.length} results for $n queued commands"))
          case other                                   =>
            errorOf(other) match {
              case Some(message) => CIO.fail(TransactionDiscarded(message))
              case None          => CIO.fail(ProtocolError(s"unexpected EXEC reply: ${Frame.describe(other)}"))
            }
        }
    }
  }

  def errorOf(frame: Frame): Option[String] =
    frame match {
      case Frame.SimpleError(message) => Some(message)
      case Frame.BulkError(message)   => Some(message.asUtf8String)
      case _                          => None
    }

  // a programmer error, deliberately outside the sealed hierarchy (like the blocking-command guard): a scope captured past its block
  def scopeReleasedError: IllegalStateException =
    new IllegalStateException("transaction scope used after its block returned")

  /**
    * N positions filled by independent callbacks, completing the single supplied callback once every position has landed. Each position is
    * set exactly once (one reply per command, or one terminal re-route outcome), so the countdown reaching zero fires the completion once —
    * no done-guard is needed. This is the collect-all shape shared by the standalone and cluster pipelines; the connection's fail-fast
    * RawBatch deliberately keeps its own.
    */
  final class IndexedCollector[A](n: Int, complete: Try[Vector[A]] => Unit) {
    private val slots     = new AtomicReferenceArray[A](n)
    private val remaining = new AtomicInteger(n)

    def set(index: Int, value: A): Unit = {
      slots.set(index, value)
      if (remaining.decrementAndGet() == 0) complete(Success(Vector.tabulate(n)(slots.get)))
    }
  }
}
