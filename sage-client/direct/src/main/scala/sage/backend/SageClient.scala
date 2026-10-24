package sage.backend

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, LoweredClient, Paged, Subscription}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.*

/**
  * A Sage client for plain blocking code. Each operation blocks the calling thread until the reply arrives and returns the result directly.
  * Failures are thrown as exceptions. Interrupting the calling thread cancels the wait, except in `close`, which always finishes. Call it from virtual threads, which makes blocking
  * cheap, for example from an HTTP handler in a framework that runs each request on a virtual thread.
  */
type SageClient = Client[[A] =>> A, String]

extension [K](client: Client[[A] =>> A, K])(using @unused ev: KeyCodec[K]) {

  /**
    * Scans the full keyspace. The returned iterator fetches pages as it is consumed, so each call to `hasNext` may send a command. An empty
    * page does not end the scan; iteration stops when the server returns a zero cursor. Redis may return the same key more than once. In
    * cluster mode, each master is scanned with its own cursor.
    */
  def scanAll(
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Iterator[K] =
    paged(Paged.scanAll[K](client.runner, pattern, count, ofType))

  /**
    * Iterates over all HSCAN field/value pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def hScanAll[F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Iterator[(F, V)] =
    paged(Paged.scanKey(client.runner)(cursor => Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * Iterates over all SSCAN members until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def sScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Iterator[V] =
    paged(Paged.scanKey(client.runner)(cursor => Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * Iterates over all ZSCAN member/score pairs until the server returns a zero cursor. An empty page with a non-zero cursor continues the scan.
    */
  def zScanAll[V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Iterator[(V, Double)] =
    paged(Paged.scanKey(client.runner)(cursor => SortedSets.zScan[K, V](key, cursor, pattern, count)))

  // convert pages from the shared Paged helper into individual iterator elements
  private def paged[S, A](pages: Paged.Pages[S, A]): Iterator[A] =
    CStream.unfold[S, Vector[A]](pages.init)(pages.step).flatMap(items => CStream.init(items)).lower

  /**
    * Lazily pages an entire stream by range, batching `XRANGE` and advancing past the last id each page. Stops when a page comes back empty.
    */
  def xRangeAll[F: KeyCodec, V: ValueCodec](
    key: K,
    start: StreamRangeId = StreamRangeId.Min,
    end: StreamRangeId = StreamRangeId.Max,
    batch: Long = 100L
  ): Iterator[StreamEntry[F, V]] =
    paged(Paged.xRangeAll[K, F, V](client.runner, key, start, end, batch))

  /**
    * Auto-claims idle pending entries for `consumer`, advancing the `XAUTOCLAIM` cursor until it returns to the start. Entries whose data
    * has already been deleted are skipped.
    */
  def xAutoClaimAll[F: KeyCodec, V: ValueCodec](
    key: K,
    group: String,
    consumer: String,
    minIdle: FiniteDuration,
    start: StreamId = StreamId.Zero,
    count: Option[Long] = None
  ): Iterator[StreamEntry[F, V]] =
    paged(Paged.xAutoClaimAll[K, F, V](client.runner, key, group, consumer, minIdle, start, count))

  /**
    * Follows a stream without a consumer group. It first reads every entry after `from`, then waits for new entries, so the iterator does
    * not end on its own. The explicit entry ID used for each blocking read avoids missing entries that arrive between reads. Unlike
    * [[xConsume]], this method does not acknowledge entries. `from` defaults to the start of the stream.
    */
  def xTail[F: KeyCodec, V: ValueCodec](
    key: K,
    from: StreamId = StreamId.Zero,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
  ): Iterator[StreamEntry[F, V]] =
    paged(Paged.xTail[K, F, V](client.runner, key, from, count, block))

  /**
    * Follows a stream as part of a consumer group and blocks the calling thread until it is interrupted or `handle` throws. It processes this
    * consumer's pending entries first, then waits for new entries. Each entry is acknowledged only after `handle` succeeds. If `handle`
    * fails, the entry remains pending and can be recovered later.
    */
  def xConsume[F: KeyCodec, V: ValueCodec](
    group: String,
    consumer: String,
    key: K,
    count: Option[Long] = None,
    block: BlockTimeout = Paged.defaultPoll
  )(handle: StreamEntry[F, V] => Unit): Unit =
    paged(Paged.xConsume[K, F, V](client.runner, group, consumer, key, count, block)).foreach { entry =>
      handle(entry)
      client.run(Streams.xAck(key, group)(entry.id)): Unit
    }

  /**
    * Subscribes to one or more channels and returns an iterator over the received messages. The subscription is open when this method
    * returns: standalone and master-replica clients wait for server confirmation, and cluster clients wait up to the connection timeout and
    * may return before confirmation. `hasNext` blocks until a message arrives. Close the iterator to unsubscribe, for example with
    * `scala.util.Using`. Sage resubscribes after reconnecting, but messages published while the connection is down are lost.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): Messages[Message[V]] =
    new Messages(client.subscribeChannels[V](channel, rest*))

  /**
    * Like [[subscribe]], but subscribes to glob patterns. Each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): Messages[PatternMessage[V]] =
    new Messages(client.subscribePatterns[V](pattern, rest*))

  /**
    * Like [[subscribe]], but subscribes to shard channels. In a cluster, each subscription follows its slot to the current owning node after a
    * migration or failover. Sharded deliveries use the ordinary [[Message]] type.
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): Messages[Message[V]] =
    new Messages(client.subscribeShardChannels[V](channel, rest*))

  /**
    * Subscribes to one or more channels and calls `handle` for each message, in order, on a virtual thread that Sage starts. Delivery
    * continues until the returned handle is closed or the client is closed. The subscription is open when this method returns, as with
    * [[subscribe]]. Use it to forward messages to code that runs in the background, for example `client.onMessage[String]("events")(publish)`.
    *
    * If `handle` throws, the failure is passed to the delivery thread's uncaught exception handler and delivery continues with the next
    * message. A payload that cannot be decoded ends the subscription and is reported the same way. The delivery thread does not inherit the
    * caller's inheritable thread-local values, so request or transaction context does not leak into it.
    *
    * Closing the returned handle unsubscribes and waits until the delivery thread has finished, so `handle` is not called after `close`
    * returns. A call to `handle` that is in progress runs to completion first. Closing more than once, or after the client was closed, has no
    * effect. Closing from inside `handle` unsubscribes without waiting.
    */
  def onMessage[V: ValueCodec](channel: String, rest: String*)(handle: Message[V] => Unit): AutoCloseable =
    deliver(subscribe[V](channel, rest*))(handle)

  /**
    * Like [[onMessage]], but subscribes to glob patterns. Each delivery names the matching pattern and the concrete channel.
    */
  def onPatternMessage[V: ValueCodec](pattern: String, rest: String*)(handle: PatternMessage[V] => Unit): AutoCloseable =
    deliver(pSubscribe[V](pattern, rest*))(handle)

  /**
    * Like [[onMessage]], but subscribes to shard channels, as [[sSubscribe]] does.
    */
  def onShardMessage[V: ValueCodec](channel: String, rest: String*)(handle: Message[V] => Unit): AutoCloseable =
    deliver(sSubscribe[V](channel, rest*))(handle)

  // Consumes `messages` on a virtual thread and calls `handle` for each message. Closing the returned handle closes `messages`.
  private def deliver[A](messages: Messages[A])(handle: A => Unit): AutoCloseable = {
    val worker = Thread
      .ofVirtual()
      .name("sage-subscription")
      .inheritInheritableThreadLocals(false)
      .unstarted { () =>
        try
          messages.foreach { message =>
            try handle(message)
            catch {
              case NonFatal(e) =>
                val thread = Thread.currentThread()
                thread.getUncaughtExceptionHandler.uncaughtException(thread, e)
            }
          }
        finally messages.close()
      }
    worker.start()
    () => {
      messages.close()
      // Keep waiting after an interrupt so that `handle` is never called after `close` returns, then restore the interrupt status.
      if (Thread.currentThread() ne worker) {
        var interrupted = false
        while (worker.isAlive)
          try worker.join()
          catch { case _: InterruptedException => interrupted = true }
        if (interrupted) Thread.currentThread().interrupt()
      }
    }
  }
}

/**
  * An open subscription read as a blocking iterator. `hasNext` waits for the next message and returns `false` once the subscription has
  * ended, either because it was closed or because the client was closed. `close` unsubscribes and can be called more than once.
  */
final class Messages[A] private[backend] (subscription: Subscription[[X] =>> X, A]) extends Iterator[A] with AutoCloseable {

  private var pending: Option[A] = None
  private var ended              = false
  private val closed             = new java.util.concurrent.atomic.AtomicBoolean(false)

  def hasNext: Boolean = {
    if (pending.isEmpty && !ended)
      subscription.next match {
        case some @ Some(_) => pending = some
        case None           => ended = true
      }
    pending.isDefined
  }

  def next(): A =
    (if (hasNext) pending else None) match {
      case Some(a) =>
        pending = None
        a
      case None    => throw new NoSuchElementException("the subscription has ended")
    }

  def close(): Unit = if (closed.compareAndSet(false, true)) subscription.close
}

object SageClient {

  /**
    * A client that uses `K` for keys, returned by `client.as[K]`. [[SageClient]] uses `String` keys by default. Calling `as` changes only
    * the key type and continues to use the same connection.
    */
  type Keyed[K] = Client[[A] =>> A, K]

  /**
    * Connects with `config` and returns the client. Call `close` when the client is no longer needed, for example when the application shuts
    * down.
    */
  def connect(config: SageConfig): SageClient = new Lowered(Client.connect(config).lower)

  /**
    * Connects with `config`, passes the client to `body`, and closes the client when `body` returns or throws. Failures while closing are
    * ignored so they cannot hide the result of `body`.
    */
  def use[A](config: SageConfig)(body: SageClient => A): A = {
    val client = connect(config)
    try body(client)
    finally
      try client.close
      catch { case NonFatal(_) => () }
  }

  final private[sage] class Lowered(underlying: Client[CIO, String]) extends LoweredClient[[X] =>> X](underlying) {
    protected def lower[A](c: CIO[A]): A = c.lower
    protected def lift[A](fa: A): CIO[A] = CIO.value(fa)
  }
}
