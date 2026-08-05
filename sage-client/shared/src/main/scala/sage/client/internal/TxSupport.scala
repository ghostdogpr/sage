package sage.client.internal

import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}

import scala.util.{Failure, Success, Try}

import kyo.compat.*

import sage.SageException
import sage.SageException.{DecodeError, ProtocolError, ServerError, TransactionDiscarded}
import sage.commands.{Command, Reply}
import sage.protocol.Frame

/**
  * The Pipeline/Transaction result-shaping shared by the standalone client and the cluster runtime, kept in one place so the
  * per-position model and the MULTI/EXEC interpretation cannot drift between them.
  */
private[internal] object TxSupport {

  def collapseStrict[Out](results: Vector[Either[SageException, Any]], toOut: Vector[Any] => Out): CIO[Out] = {
    val values = Vector.newBuilder[Any]
    values.sizeHint(results.length)
    val it     = results.iterator
    while (it.hasNext)
      it.next() match {
        case Right(value) => values += value
        case Left(error)  => return CIO.fail(error)
      }
    CIO.value(toOut(values.result()))
  }

  // decoders should return Either; another exception indicates a decoder bug and becomes DecodeError to preserve per-command results
  def toEither(result: Try[Any]): Either[SageException, Any] =
    result match {
      case Success(value)            => Right(value)
      case Failure(e: SageException) => Left(e)
      case Failure(other)            => Left(DecodeError.fromThrowable(other))
    }

  // Return None when EXEC reports that a watched key changed. Otherwise, return each command's decoded result. A queueing error fails the
  // effect before the transaction executes.
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

  // Return error frames from either the top-level transaction replies or the array returned by EXEC.
  def execErrors(frames: Vector[Frame]): Iterator[ServerError] = {
    val nested = frames.lastOption match {
      case Some(Frame.Array(elems)) => elems.iterator
      case _                        => Iterator.empty[Frame]
    }
    (frames.iterator ++ nested).flatMap(errorOf).map(ServerError.of)
  }

  // using a transaction scope after its block ends is an invalid state and returns IllegalStateException instead of a SageException
  def scopeReleasedError: IllegalStateException =
    new IllegalStateException("transaction scope used after its block returned")

  /**
    * Collects results from independent callbacks by their original index. Each index is set once, either by its command reply or its final
    * routing result. When all indices are set, the countdown invokes `complete` once. Standalone and cluster pipelines use this collector
    * because they wait for every result. `RawBatch` uses separate logic because it completes after the first failure.
    */
  final class IndexedCollector[A](n: Int, complete: Vector[A] => Unit) {
    private val slots     = new AtomicReferenceArray[A](n)
    private val remaining = new AtomicInteger(n)

    def set(index: Int, value: A): Unit = {
      slots.set(index, value)
      if (remaining.decrementAndGet() == 0) complete(Vector.tabulate(n)(slots.get))
    }
  }
}
