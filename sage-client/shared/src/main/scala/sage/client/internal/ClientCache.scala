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

  private val lock            = new ReentrantLock()
  // accessOrder = true moves a read entry to the end of the map. Eviction then removes the least recently used entry first.
  private val entries         = new java.util.LinkedHashMap[Key, Entry](16, 0.75f, true)
  private val reverse         = mutable.HashMap.empty[Key, mutable.HashSet[Key]]
  private val pending         = mutable.HashMap.empty[Key, Fetching]
  private var bytesUsed: Long = 0L
  // a flush retires every hit handed out before it
  @volatile private var epoch = 0L

  /**
    * Tries to serve `commandBytes` from the cache. [[Hit]] returns the stored frame. Decode it and complete the caller. [[Fetch]] means that
    * this caller is the first to miss. Read from the server, then pass its ticket to [[store]] or [[fail]]. [[Wait]] means that another fetch is in flight
    * and now owns `waiter`. Do nothing for this caller. [[Fetch]] and [[Wait]] enqueue `waiter`; [[Hit]] does not.
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
        case Some(fetching) =>
          fetching.waiters += waiter
          Wait
        case None           =>
          val fetching = new Fetching(key, trackedKeys.map(new Key(_)))
          fetching.waiters += waiter
          pending.update(key, fetching)
          Fetch(fetching)
      }
    } finally lock.unlock()
  }

  // Waiters are read after the unlock: once the fetch leaves `pending`, no acquire can add to them.
  def store(fetching: Fetching, frame: Frame, now: Long, ttlMillis: Long): Unit = {
    val size = frameSize(frame) // walked outside the lock so a large reply can't stall acquire/invalidate
    lock.lock()
    try {
      pending.remove(fetching.key)
      // An entry larger than the cache limit cannot be stored, so return its reply without caching it.
      if (!fetching.dirty && size <= maxBytes) insert(fetching.key, new Entry(frame, size, now + ttlMillis, fetching.keys))
    } finally lock.unlock()
    fetching.waiters.foreach(_.apply(Success(frame)))
  }

  def fail(fetching: Fetching, error: Throwable): Unit = {
    lock.lock()
    try pending.remove(fetching.key)
    finally lock.unlock()
    fetching.waiters.foreach(_.apply(Failure(error)))
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
      pending.valuesIterator.foreach(fetching => if (fetching.keys.contains(tracked)) fetching.dirty = true)
    } finally lock.unlock()
  }

  def flush(): Unit = {
    lock.lock()
    try {
      entries.clear()
      reverse.clear()
      bytesUsed = 0L
      pending.valuesIterator.foreach(_.dirty = true)
      epoch += 1
    } finally lock.unlock()
  }

  // false once a flush has retired the hit; the caller looks the command up again
  def isCurrent(hit: Hit): Boolean = hit.epoch == epoch

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

  // This wrapper compares Bytes by content. Bytes itself uses reference equality for `==` and `hashCode`; see Bytes.
  final class Key(val bytes: Bytes) {
    private val hash                         = bytes.contentHashCode
    override def hashCode(): Int             = hash
    override def equals(other: Any): Boolean = other match {
      case that: Key => bytes.sameBytes(that.bytes)
      case _         => false
    }
  }

  enum Acquire {
    case Hit(frame: Frame, epoch: Long)
    case Fetch(ticket: Fetching)
    case Wait
  }

  final private class Entry(val frame: Frame, val sizeBytes: Long, val expiresAt: Long, val keys: Vector[Key])

  // the in-flight server read that the first missing caller owns; guarded by the cache lock until it leaves `pending`
  final class Fetching private[ClientCache] (private[ClientCache] val key: Key, private[ClientCache] val keys: Vector[Key]) {
    private[ClientCache] val waiters        = mutable.ArrayBuffer.empty[Try[Frame] => Unit]
    private[ClientCache] var dirty: Boolean = false
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
