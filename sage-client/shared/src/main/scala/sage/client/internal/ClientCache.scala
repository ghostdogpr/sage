package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.protocol.Frame

/**
  * Stores cached replies for one [[MultiplexedConnection]] generation, indexed by the complete encoded command. A reverse index maps each
  * tracked key to the entries that invalidation must remove. Concurrent misses share one server request. Entries expire when read after
  * their TTL, and least-recently-used entries are evicted to stay within the byte limit. Reconnecting creates a new cache. Tests pass `now`
  * explicitly to control expiry, and callbacks run outside the lock. If invalidation or a flush arrives during a fetch, its reply is returned
  * to waiting callers but is not cached.
  */
final private[client] class ClientCache(maxBytes: Long) {
  import ClientCache.*
  import ClientCache.Acquire.*

  private val lock                                   = new ReentrantLock()
  // accessOrder = true moves a read entry to the end of the map. Eviction then removes the least recently used entry first.
  private val entries                                = new java.util.LinkedHashMap[Key, Entry](16, 0.75f, true)
  private val reverse                                = mutable.HashMap.empty[Key, mutable.HashSet[Key]]
  private val pending                                = mutable.HashMap.empty[Key, InFlight]
  private var bytesUsed: Long                        = 0L
  @volatile private var epoch: CacheEpoch            = CacheEpoch.initial
  @volatile private var rerouteWatermark: CacheEpoch = CacheEpoch.initial

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
        if (entry.expiresAt > now) return Hit(entry.frame, epoch)
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
    var waiters: mutable.ArrayBuffer[Try[Frame] => Unit] = null
    lock.lock()
    try {
      val inFlight = pending.remove(key)
      waiters = inFlight.map(_.waiters).orNull
      val dirty    = inFlight.exists(_.dirty)
      // Reuse the Key objects created by the matching acquire. Create them here only when no matching fetch is recorded. An entry larger than
      // the cache limit cannot be stored, so return its reply without caching it.
      if (!dirty && size <= maxBytes)
        insert(key, new Entry(frame, size, now + ttlMillis, inFlight.map(_.keys).getOrElse(trackedKeys.map(new Key(_)))))
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
      clearEntries()
      epoch = epoch.next
    } finally lock.unlock()
  }

  def flushForReroute(): Unit = {
    lock.lock()
    try {
      clearEntries()
      val retired = epoch.next
      // publish the watermark before the epoch, which readers check first. This prevents a reader from pairing the new epoch with the old watermark.
      rerouteWatermark = retired
      epoch = retired
    } finally lock.unlock()
  }

  private def clearEntries(): Unit = {
    entries.clear()
    reverse.clear()
    bytesUsed = 0L
    pending.valuesIterator.foreach(_.dirty = true)
  }

  def isCurrent(stamped: CacheEpoch): Boolean = epoch == stamped

  def rerouteRetired(stamped: CacheEpoch): Boolean = rerouteWatermark.isAfter(stamped)

  private def insert(key: Key, entry: Entry): Unit = {
    val previous = entries.put(key, entry)
    // drop the replaced entry before recording the new mappings, so a key shared by both is not removed right after being re-added
    if (previous != null) dropAccounting(key, previous)
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
        if (set.isEmpty) { reverse.remove(k): Unit }
      }
    }
}

private[client] object ClientCache {

  // a content-addressed Bytes key — universal `==`/`hashCode` on Bytes is reference-based by design (see Bytes)
  final class Key(val bytes: Bytes) {
    private val hash                         = bytes.contentHashCode
    override def hashCode(): Int             = hash
    override def equals(other: Any): Boolean = other match {
      case that: Key => bytes.sameBytes(that.bytes)
      case _         => false
    }
  }

  opaque type CacheEpoch = Long
  object CacheEpoch {
    val initial: CacheEpoch = 0L
  }
  extension (e: CacheEpoch) {
    def next: CacheEpoch                    = e + 1L
    def isAfter(other: CacheEpoch): Boolean = e > other
  }

  enum Acquire {
    case Hit(frame: Frame, epoch: CacheEpoch)
    case Fetch
    case Wait
  }

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
