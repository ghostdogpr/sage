package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.protocol.Frame

/**
  * The per-[[MultiplexedConnection]] generation's client-side cache: keyed by full command wire bytes, with a reverse index from each
  * tracked key for invalidation, single-flight on concurrent misses, lazy absolute-TTL expiry, and a bytes-bounded LRU. A reconnect
  * discards the instance — that is how a reconnect flushes the cache. `now` is passed in so tests drive expiry; callbacks run outside the
  * lock. An invalidation or flush arriving mid-fetch marks that fetch dirty: its reply still reaches waiters but is not stored.
  */
final private[client] class ClientCache(maxBytes: Long) {
  import ClientCache.*

  private val lock            = new ReentrantLock()
  // accessOrder = true: a lookup promotes the entry to most-recently-used, so eviction sheds the genuinely cold entries
  private val entries         = new java.util.LinkedHashMap[Key, Entry](16, 0.75f, true)
  private val reverse         = mutable.HashMap.empty[Key, mutable.HashSet[Key]]
  private val pending         = mutable.HashMap.empty[Key, InFlight]
  private var bytesUsed: Long = 0L

  /**
    * Tries to serve `commandBytes` from cache. [[Hit]] returns the stored frame (decode it and complete the caller). [[Fetch]] means the
    * caller is the first to miss and must issue the server read, then call [[store]] (or [[fail]]). [[Wait]] means another fetch is in
    * flight and `waiter` has been enqueued onto it — do nothing. The `waiter` is enqueued for [[Fetch]] and [[Wait]], not for [[Hit]].
    */
  def acquire(commandBytes: Bytes, trackedKeys: Vector[Bytes], now: Long, waiter: Try[Frame] => Unit): Acquire = {
    lock.lock()
    try {
      val key   = new Key(commandBytes)
      val entry = entries.get(key)
      if (entry != null) {
        if (entry.expiresAt > now) return Hit(entry.frame)
        removeEntry(key, entry)
      }
      pending.get(key) match {
        case Some(inFlight) =>
          inFlight.waiters += waiter
          Wait
        case None           =>
          val inFlight = new InFlight(trackedKeys.map(new Key(_)))
          inFlight.waiters += waiter
          pending.update(key, inFlight)
          Fetch
      }
    } finally lock.unlock()
  }

  def store(commandBytes: Bytes, trackedKeys: Vector[Bytes], frame: Frame, now: Long, ttlMillis: Long): Unit = {
    val key                                              = new Key(commandBytes)
    val size                                             = frameSize(frame) // walked outside the lock so a large reply can't stall acquire/invalidate
    val tracked                                          = trackedKeys.map(new Key(_))
    var waiters: mutable.ArrayBuffer[Try[Frame] => Unit] = null
    lock.lock()
    try {
      val inFlight = pending.remove(key)
      waiters = inFlight.map(_.waiters).orNull
      val dirty    = inFlight.exists(_.dirty)
      // an entry larger than the whole cap would evict everything then itself; skip caching it and just deliver the reply
      if (!dirty && size <= maxBytes) insert(key, new Entry(frame, size, now + ttlMillis, tracked))
    } finally lock.unlock()
    if (waiters != null) waiters.foreach(_.apply(Success(frame)))
  }

  def fail(commandBytes: Bytes, error: Throwable): Unit = {
    val key                                              = new Key(commandBytes)
    var waiters: mutable.ArrayBuffer[Try[Frame] => Unit] = null
    lock.lock()
    try waiters = pending.remove(key).map(_.waiters).orNull
    finally lock.unlock()
    if (waiters != null) waiters.foreach(_.apply(Failure(error)))
  }

  def invalidate(redisKey: Bytes): Unit = {
    val tracked = new Key(redisKey)
    lock.lock()
    try {
      reverse.remove(tracked).foreach { keys =>
        keys.foreach { ck =>
          val entry = entries.get(ck)
          if (entry != null) removeEntry(ck, entry)
        }
      }
      pending.valuesIterator.foreach(inFlight => if (inFlight.keys.contains(tracked)) inFlight.dirty = true)
    } finally lock.unlock()
  }

  def flush(): Unit = {
    lock.lock()
    try {
      entries.clear()
      reverse.clear()
      bytesUsed = 0L
      pending.valuesIterator.foreach(_.dirty = true)
    } finally lock.unlock()
  }

  private def insert(key: Key, entry: Entry): Unit = {
    val previous = entries.put(key, entry)
    if (previous != null) bytesUsed -= previous.sizeBytes
    entry.keys.foreach(k => reverse.getOrElseUpdate(k, mutable.HashSet.empty) += key)
    bytesUsed += entry.sizeBytes
    val it       = entries.entrySet().iterator()
    while (bytesUsed > maxBytes && it.hasNext) {
      val evicted = it.next()
      it.remove()
      dropAccounting(evicted.getKey, evicted.getValue)
    }
  }

  private def removeEntry(key: Key, entry: Entry): Unit = {
    entries.remove(key)
    dropAccounting(key, entry)
  }

  private def dropAccounting(key: Key, entry: Entry): Unit = {
    bytesUsed -= entry.sizeBytes
    removeReverse(key, entry)
  }

  private def removeReverse(key: Key, entry: Entry): Unit =
    entry.keys.foreach { k =>
      reverse.get(k).foreach { set =>
        set -= key
        if (set.isEmpty) { val _ = reverse.remove(k) }
      }
    }
}

private[client] object ClientCache {

  // a content-addressed Bytes key — universal `==`/`hashCode` on Bytes is reference-based by design (see Bytes)
  final class Key(val bytes: Bytes) {
    override def hashCode(): Int             = bytes.contentHashCode
    override def equals(other: Any): Boolean = other match {
      case that: Key => bytes.sameBytes(that.bytes)
      case _         => false
    }
  }

  sealed trait Acquire
  final case class Hit(frame: Frame) extends Acquire
  case object Fetch                  extends Acquire
  case object Wait                   extends Acquire

  final private class Entry(val frame: Frame, val sizeBytes: Long, val expiresAt: Long, val keys: Vector[Key])

  final private class InFlight(val keys: Vector[Key]) {
    val waiters        = mutable.ArrayBuffer.empty[Try[Frame] => Unit]
    var dirty: Boolean = false
  }

  // approximate retained size: payload bytes plus a flat per-node overhead, enough to bound memory without walking object headers exactly
  private def frameSize(frame: Frame): Long =
    frame match {
      case Frame.BulkString(b)        => 16L + b.length
      case Frame.BulkError(b)         => 16L + b.length
      case Frame.VerbatimString(_, b) => 16L + b.length
      case Frame.SimpleString(s)      => 16L + s.length
      case Frame.SimpleError(s)       => 16L + s.length
      case Frame.Array(elements)      => 16L + elements.foldLeft(0L)((acc, e) => acc + frameSize(e))
      case Frame.Set(elements)        => 16L + elements.foldLeft(0L)((acc, e) => acc + frameSize(e))
      case Frame.Push(elements)       => 16L + elements.foldLeft(0L)((acc, e) => acc + frameSize(e))
      case Frame.Map(entries)         => 16L + entries.foldLeft(0L)((acc, kv) => acc + frameSize(kv._1) + frameSize(kv._2))
      case _                          => 16L
    }
}
