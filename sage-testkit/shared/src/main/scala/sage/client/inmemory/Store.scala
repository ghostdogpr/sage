package sage.client.inmemory

import java.util.concurrent.locks.ReentrantLock

import scala.collection.immutable.{TreeSet, VectorMap}
import scala.collection.mutable
import scala.util.control.NoStackTrace

import sage.Bytes
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

  // fields keep their insertion order, as small hashes do on a real server
  final case class Hash(fields: VectorMap[Key, Bytes]) extends Value {
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

  object Sorted {

    // -0.0 and 0.0 must sort as equal scores, so the comparison uses == rather than java.lang.Double.compare
    given Ordering[Double] = (a, b) => if (a < b) -1 else if (a > b) 1 else 0

    val empty: Sorted = Sorted(Map.empty, TreeSet.empty[(Double, Key)])
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
  // a time no clock reaches, since -1 is a valid expiry for a clock set before 1970
  val NoExpiry: Long = Long.MinValue
}

/**
  * How a write sets the expiry of a key: it keeps the current expiry, removes it, or sets it to a Unix time in milliseconds.
  */
private[inmemory] enum Ttl {
  case Keep, Clear
  case At(millis: Long)

  def resolve(current: Long): Long =
    this match {
      case Keep       => current
      case Clear      => Entry.NoExpiry
      case At(millis) => millis
    }
}

/**
  * Stops a command and replies with the error `frame`. Command handlers throw it from argument parsing and type checks.
  */
final private[inmemory] class Reject(val frame: Frame.SimpleError) extends RuntimeException with NoStackTrace

/**
  * The keyspace of one in-memory server. Every method must be called while holding `lock`, which [[Executor]] takes for each command, so
  * each command runs atomically like it does on a real server.
  *
  * Expired keys are removed lazily, when a command reads them. Every change to a key that a server's `WATCH` sees increases that key's
  * version, which `WATCH` compares to decide whether a transaction must abort. Versions are kept after a key is deleted because watching a
  * missing key and then creating it must also abort the transaction.
  */
final private[sage] class Store {

  val lock = new ReentrantLock()

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

  // removes the key when it has expired
  private def current(key: Key, entry: Entry, now: Long): Option[Entry] =
    if (!entry.expiredAt(now)) Some(entry)
    else {
      data.remove(key)
      touch(key)
      None
    }

  /**
    * Stores `value` at `key` with the expiry that `ttl` gives, or deletes the key when `value` is an empty collection. An existing key keeps
    * its `SCAN` position.
    */
  def put(key: Key, value: Value, ttl: Ttl, now: Long): Unit = {
    val existing = lookup(key, now)
    if (!value.isEmpty) {
      data.update(key, Entry(value, ttl.resolve(existing.fold(Entry.NoExpiry)(_.expiresAt)), existing.fold(nextSeq())(_.seq)))
      touch(key)
    } else if (existing.isDefined) {
      data.remove(key)
      touch(key)
    }
  }

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
    data.keysIterator.foreach(touch(_))
    data.clear()
  }

  // removes the key first when it has expired, so that an expiry counts as a change for WATCH, as it does on a server
  def version(key: Key, now: Long): Long = {
    lookup(key, now): Unit
    versions.getOrElse(key, 0L)
  }

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
