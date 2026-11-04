package sage.client.inmemory

import java.util.concurrent.locks.ReentrantLock

import scala.collection.immutable.{TreeMap, TreeSet, VectorMap}
import scala.collection.mutable
import scala.util.control.NoStackTrace

import sage.Bytes
import sage.commands.StreamId
import sage.protocol.Frame

/**
  * A key or other binary argument used as a map key. `Bytes` is an array, so it needs content-based equality to work in a `HashMap`.
  */
final private[inmemory] class Key(val bytes: Bytes) {

  override def equals(that: Any): Boolean =
    that match {
      case other: Key => bytes.sameBytes(other.bytes)
      case _          => false
    }

  override def hashCode(): Int = bytes.contentHashCode

  override def toString: String = bytes.asUtf8String
}

private[inmemory] object Key {

  // Redis compares members and keys as unsigned bytes, for example in lexicographic sorted-set ranges
  given Ordering[Key] = (a, b) => java.util.Arrays.compareUnsigned(a.bytes.unsafeArray, b.bytes.unsafeArray)
}

/**
  * The data held at one key. Each case corresponds to one Redis data type and reports the name that `TYPE` returns.
  */
sealed private[inmemory] trait Value {

  def typeName: String

  // Redis deletes a key when its collection becomes empty
  def isEmpty: Boolean = false
}

private[inmemory] object Value {

  final case class Str(bytes: Bytes) extends Value {
    def typeName: String = "string"
  }

  /**
    * A hash field and its own expiry time, using [[Entry.NoExpiry]] when it has none. Fields keep their insertion order, as small hashes
    * do on a real server.
    */
  final case class Field(value: Bytes, expiresAt: Long) {
    def hasExpiry: Boolean            = expiresAt != Entry.NoExpiry
    def expiredAt(now: Long): Boolean = hasExpiry && expiresAt <= now
  }

  final case class Hash(fields: VectorMap[Key, Field]) extends Value {
    def typeName: String          = "hash"
    override def isEmpty: Boolean = fields.isEmpty
  }

  final case class Lst(items: Vector[Bytes]) extends Value {
    def typeName: String          = "list"
    override def isEmpty: Boolean = items.isEmpty
  }

  final case class Members(members: Set[Key]) extends Value {
    def typeName: String          = "set"
    override def isEmpty: Boolean = members.isEmpty
  }

  /**
    * A sorted set. `scores` finds a member's score, and `ordered` keeps members sorted by score and then by their bytes, which is the
    * order Redis uses.
    */
  final case class Sorted(scores: Map[Key, Double], ordered: TreeSet[(Double, Key)]) extends Value {

    def typeName: String          = "zset"
    override def isEmpty: Boolean = scores.isEmpty

    def updated(member: Key, score: Double): Sorted = {
      val without = scores.get(member).fold(ordered)(old => ordered - (old -> member))
      Sorted(scores.updated(member, score), without + (score -> member))
    }

    def removed(member: Key): Sorted =
      scores.get(member).fold(this)(old => Sorted(scores - member, ordered - (old -> member)))
  }

  /**
    * A stream. `entries` holds the field/value pairs of each entry in id order. `lastId` is the last id the stream generated or was set
    * to, which can be greater than the last remaining entry, and `maxDeletedId` is the greatest id removed so far. An empty stream keeps
    * its key, as on a real server.
    */
  final case class Stream(
    entries: TreeMap[StreamId, Vector[(Bytes, Bytes)]],
    lastId: StreamId,
    entriesAdded: Long,
    maxDeletedId: StreamId,
    groups: VectorMap[String, Group]
  ) extends Value {
    def typeName: String = "stream"
  }

  object Stream {
    val empty: Stream = Stream(TreeMap.empty, StreamId.Zero, 0L, StreamId.Zero, VectorMap.empty)
  }

  /**
    * A consumer group. `pending` is the group's pending entries list, and `entriesRead` is how many entries the group has read when that
    * number is known, which `XINFO` uses to report the lag.
    */
  final case class Group(
    lastDelivered: StreamId,
    entriesRead: Option[Long],
    pending: TreeMap[StreamId, Pending],
    consumers: VectorMap[String, Consumer]
  )

  // `deliveredAt` is a Unix time in milliseconds
  final case class Pending(consumer: String, deliveredAt: Long, deliveries: Long)

  // `activeAt` is -1 until the consumer has successfully read or claimed an entry
  final case class Consumer(seenAt: Long, activeAt: Long)

  object Sorted {

    // -0.0 and 0.0 must sort as equal scores, so the comparison uses == rather than java.lang.Double.compare
    private given Ordering[Double] = (a, b) => if (a < b) -1 else if (a > b) 1 else 0

    val empty: Sorted = Sorted(Map.empty, TreeSet.empty[(Double, Key)])

    def of(pairs: Iterable[(Key, Double)]): Sorted = pairs.foldLeft(empty)((set, pair) => set.updated(pair._1, pair._2))
  }
}

/**
  * The value stored at one key. `expiresAt` is a Unix time in milliseconds, or [[Entry.NoExpiry]]. `seq` is assigned when the key is
  * created and orders keys for `SCAN`, so a key that exists for the whole iteration is returned even when other keys are added or removed.
  */
final private[inmemory] case class Entry(value: Value, expiresAt: Long, seq: Long) {

  def hasExpiry: Boolean = expiresAt != Entry.NoExpiry

  def expiredAt(now: Long): Boolean = hasExpiry && expiresAt <= now
}

private[inmemory] object Entry {
  val NoExpiry: Long = -1L
}

/**
  * Stops a command and replies with `frame`, which is usually an error. Command handlers throw it from argument parsing and type checks.
  */
final private[inmemory] class Reject(val frame: Frame) extends RuntimeException with NoStackTrace

/**
  * The keyspace of one in-memory server. Every method must be called while holding `lock`, which [[Executor]] takes for each command, so
  * each command runs atomically like it does on a real server.
  *
  * Expired keys are removed lazily, when a command reads them. Every change to a key increases that key's version, which `WATCH` compares
  * to decide whether a transaction must abort. Versions are kept after a key is deleted because watching a missing key and then creating
  * it must also abort the transaction.
  */
final private[sage] class Store {

  val lock = new ReentrantLock()

  // the SHA-1 digests of the cached Lua scripts; FLUSHALL keeps them, as on a real server
  val scripts: mutable.Set[String] = mutable.Set.empty

  val pubsub: Broker = new Broker(this)

  private val deferred = mutable.ArrayBuffer.empty[() => Unit]

  /**
    * Schedules `action` to run once the current holder releases the lock. Actions that complete a waiting effect run here, so that the
    * code they resume never runs while the store is locked. [[Executor.locked]] runs them.
    */
  def afterUnlock(action: () => Unit): Unit = deferred += action

  def takeDeferred(): Vector[() => Unit] = {
    val actions = deferred.toVector
    deferred.clear()
    actions
  }

  private val data     = mutable.HashMap.empty[Key, Entry]
  private val versions = mutable.HashMap.empty[Key, Long]
  private var changes  = 0L
  private var created  = 0L

  def lookup(key: Key, now: Long): Option[Entry] = data.get(key).flatMap(current(key, _, now))

  // Removes the key when it has expired, and removes expired hash fields, deleting the hash when none are left.
  private def current(key: Key, entry: Entry, now: Long): Option[Entry] =
    entry.value match {
      case _ if entry.expiredAt(now)                                            =>
        data.remove(key)
        touch(key)
        None
      case Value.Hash(fields) if fields.valuesIterator.exists(_.expiredAt(now)) =>
        val live = fields.filter((_, field) => !field.expiredAt(now))
        touch(key)
        if (live.isEmpty) {
          data.remove(key)
          None
        } else {
          val updated = entry.copy(value = Value.Hash(live))
          data.update(key, updated)
          Some(updated)
        }
      case _                                                                    => Some(entry)
    }

  /**
    * Stores `value` at `key`. An existing key keeps its `SCAN` position.
    */
  def put(key: Key, value: Value, expiresAt: Long, now: Long): Unit = {
    val seq = lookup(key, now).fold(nextSeq())(_.seq)
    data.update(key, Entry(value, expiresAt, seq))
    touch(key)
  }

  /**
    * Stores a new version of the collection at `key`, keeping the key's expiry, or deletes the key when the collection is empty.
    */
  def replace(key: Key, value: Value, now: Long): Unit =
    if (value.isEmpty) delete(key, now): Unit
    else put(key, value, lookup(key, now).fold(Entry.NoExpiry)(_.expiresAt), now)

  def setExpiry(key: Key, entry: Entry, expiresAt: Long): Unit = {
    data.update(key, entry.copy(expiresAt = expiresAt))
    touch(key)
  }

  def delete(key: Key, now: Long): Boolean =
    lookup(key, now).isDefined && {
      data.remove(key)
      touch(key)
      true
    }

  /**
    * Every key that has not expired, after removing the expired ones.
    */
  def live(now: Long): Vector[(Key, Entry)] = data.toVector.flatMap((key, entry) => current(key, entry, now).map(key -> _))

  def clear(): Unit = {
    data.keysIterator.foreach(touch)
    data.clear()
  }

  def version(key: Key): Long = versions.getOrElse(key, 0L)

  // increases on every change to any key
  def changeCount: Long = changes

  private def touch(key: Key): Unit = {
    changes += 1
    versions.update(key, changes)
  }

  private def nextSeq(): Long = {
    created += 1
    created
  }
}
