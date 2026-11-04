package sage.client.inmemory

import java.util.Locale

import scala.collection.mutable
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

import sage.Bytes
import sage.codec.Doubles
import sage.protocol.Frame

/**
  * One command being executed: its arguments, the store, and the time at which it runs. All commands in a transaction see the same `now`,
  * as they do on a real server. The parsing helpers throw [[Reject]] with the error a real server would return.
  */
final private[inmemory] class Call(val name: String, val args: Vector[Bytes], val store: Store, val now: Long) {

  def key(index: Int): Key = new Key(args(index))

  def text(index: Int): String = args(index).asUtf8String

  def keyword(index: Int): String = text(index).toUpperCase(Locale.ROOT)

  def long(index: Int): Long = Numbers.parseLong(args(index)).getOrElse(throw new Reject(Errors.notInteger))

  def lookup(key: Key): Option[Entry] = store.lookup(key, now)

  def double(index: Int): Double = Numbers.parseDouble(args(index)).getOrElse(throw new Reject(Errors.notFloat))

  /**
    * The value of type `T` stored at `key`, or `None` when the key is missing. Fails with `WRONGTYPE` when the key holds another type.
    */
  def typed[T <: Value](key: Key)(using tag: ClassTag[T]): Option[T] =
    lookup(key).map(entry =>
      entry.value match {
        case value: T => value
        case _        => throw new Reject(Errors.wrongType)
      }
    )

  def string(key: Key): Option[Bytes] = typed[Value.Str](key).map(_.bytes)

  // the store has already removed expired fields when it returns a hash
  def hash(key: Key): Option[Value.Hash] = typed[Value.Hash](key)

  /**
    * Parses `numkeys key…` starting at `index` and returns the keys and the index after them.
    */
  def numKeys(index: Int): (Vector[Key], Int) = {
    val count = long(index)
    if (count <= 0) throw new Reject(Errors.err("numkeys should be greater than 0"))
    if (index + 1 + count > args.length) throw new Reject(Errors.syntax)
    (Vector.tabulate(count.toInt)(i => key(index + 1 + i)), index + 1 + count.toInt)
  }

  def replace(key: Key, value: Value): Unit = store.replace(key, value, now)
}

/**
  * Describes one supported command. `arity` follows the convention of Redis `COMMAND INFO`: it counts the command name, a positive value
  * is an exact count, and a negative value is a minimum.
  */
final private[inmemory] case class Spec(
  arity: Int,
  run: Call => Frame,
  timeout: Option[Vector[Bytes] => Option[FiniteDuration]] = None,
  prepare: Option[Call => Vector[Bytes]] = None
)

/**
  * Parses the timeout of a blocking command. `None` means waiting forever, which a timeout of 0 requests. An invalid timeout throws
  * [[Reject]] with the server's error.
  */
private[inmemory] object Timeouts {

  // BLPOP, BLMOVE, BZPOPMIN and the other list and sorted-set forms take seconds, possibly fractional
  def seconds(arg: Bytes): Option[FiniteDuration] =
    Numbers.parseDouble(arg) match {
      case None                         => throw new Reject(Errors.err("timeout is not a float or out of range"))
      case Some(seconds) if seconds < 0 => throw new Reject(Errors.err("timeout is negative"))
      case Some(seconds)                => Option.when(seconds > 0)(math.ceil(seconds * 1000).toLong.millis)
    }

  // XREAD and XREADGROUP take whole milliseconds after BLOCK
  def millis(arg: Bytes): Option[FiniteDuration] =
    Numbers.parseLong(arg) match {
      case None               => throw new Reject(Errors.err("timeout is not an integer or out of range"))
      case Some(ms) if ms < 0 => throw new Reject(Errors.err("timeout is negative"))
      case Some(ms)           => Option.when(ms > 0)(ms.millis)
    }
}

/**
  * Thrown by a blocking command such as `BLPOP` when there is nothing to return yet. The client then waits for another write and runs the
  * command again. Outside of that path, for example inside `EXEC`, the command replies with null at once, as it does on a real server.
  */
private[inmemory] object Blocked extends RuntimeException with NoStackTrace

private[inmemory] object Errors {

  def err(message: String): Frame = Frame.SimpleError(s"ERR $message")

  val syntax: Frame     = err("syntax error")
  val notInteger: Frame = err("value is not an integer or out of range")
  val notFloat: Frame   = err("value is not a valid float")
  val noSuchKey: Frame  = err("no such key")
  val outOfRange: Frame = err("value is out of range, must be positive")
  val wrongType: Frame  = Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")

  def arity(name: String): Frame = err(s"wrong number of arguments for '${name.toLowerCase(Locale.ROOT)}' command")

  def invalidExpire(name: String): Frame = err(s"invalid expire time in '${name.toLowerCase(Locale.ROOT)}' command")

  def unknown(name: String): Frame = err(s"unknown command '$name': the in-memory client does not support it")
}

private[inmemory] object Replies {

  val ok: Frame = Frame.SimpleString("OK")

  def bulk(bytes: Bytes): Frame = Frame.BulkString(bytes)

  def bulk(text: String): Frame = Frame.BulkString(Bytes.utf8(text))

  def int(value: Long): Frame = Frame.Integer(value)

  def flag(value: Boolean): Frame = Frame.Integer(if (value) 1L else 0L)

  def optional(bytes: Option[Bytes]): Frame = bytes.fold(Frame.Null)(Frame.BulkString(_))

  def bulks(items: Iterable[Bytes]): Frame = Frame.Array(items.iterator.map(Frame.BulkString(_)).toVector)

  def keys(items: Iterable[Key]): Frame = Frame.Array(items.iterator.map(k => Frame.BulkString(k.bytes)).toVector)

  // a score inside a bulk string, as ZSCAN and ZINCRBY's RESP2 form write it
  def scoreText(score: Double): Frame = bulk(Numbers.formatScore(score))
}

private[inmemory] object Numbers {

  /**
    * Parses a decimal integer with the same strictness as Redis: an optional minus sign, no plus sign, no surrounding spaces, and no
    * leading zeros.
    */
  def parseLong(bytes: Bytes): Option[Long] = {
    val text = bytes.asUtf8String
    val ok   =
      text.nonEmpty && text.length <= 20 && {
        val digits = if (text.charAt(0) == '-') text.substring(1) else text
        digits.nonEmpty && digits.forall(c => c >= '0' && c <= '9') && (digits == "0" || digits.charAt(0) != '0') && text != "-0"
      }
    if (ok) text.toLongOption else None
  }

  /**
    * Parses a floating-point argument or stored value. Java's parser also accepts surrounding spaces and type suffixes such as `1d`, which
    * Redis rejects, so those are refused first.
    */
  def parseDouble(bytes: Bytes): Option[Double] = {
    val text = bytes.asUtf8String
    val ok   = text.nonEmpty && !text.exists(_.isWhitespace) && !text.last.isLetter || text.equalsIgnoreCase("inf") ||
      text.equalsIgnoreCase("+inf") || text.equalsIgnoreCase("-inf")
    if (ok) Doubles.parse(text.toLowerCase(Locale.ROOT)).filterNot(_.isNaN) else None
  }

  def formatScore(value: Double): String =
    if (value.isPosInfinity) "inf" else if (value.isNegInfinity) "-inf" else formatDouble(value)

  /**
    * Formats a double the way `INCRBYFLOAT` does: plain decimal notation with no exponent and no trailing zeros.
    */
  def formatDouble(value: Double): String = {
    val plain = new java.math.BigDecimal(value.toString).stripTrailingZeros.toPlainString
    if (plain == "-0") "0" else plain
  }
}

/**
  * Runs commands against a [[Store]]. Each call to [[execute]] or [[executeAll]] holds the store lock for its whole duration.
  */
final private[sage] class Executor(val store: Store) {

  private val specs: Map[String, Spec] =
    StringCommands.specs ++ KeyCommands.specs ++ ServerCommands.specs ++ HashCommands.specs ++ ListCommands.specs ++ SetCommands.specs ++
      SortedSetCommands.specs ++ Scripts.specs ++ PubSubCommands.specs ++ StreamCommands.specs ++ BitmapCommands.specs ++ HyperLogLogCommands.specs ++
      GeoCommands.specs

  /**
    * Runs one command at `now`, a Unix time in milliseconds.
    */
  def execute(name: String, args: Vector[Bytes], now: Long): Frame = locked(run(name, args, now, canBlock = false))

  /**
    * Runs a blocking command at `now`. When it has nothing to return yet, registers `onChange` to be called after the next write and
    * returns `None`. Both happen under the store lock, so a write cannot slip in between and be missed.
    */
  def attemptBlocking(name: String, args: Vector[Bytes], now: Long, onChange: () => Unit): Option[Frame] =
    locked {
      try Some(run(name, args, now, canBlock = true))
      catch {
        case Blocked =>
          waiters += onChange
          None
      }
    }

  /**
    * The timeout of a blocking command, `None` for one that waits forever, or the error frame for an invalid timeout or command.
    */
  def blockTimeout(name: String, args: Vector[Bytes]): Either[Frame, Option[FiniteDuration]] =
    specs.get(name.toUpperCase(Locale.ROOT)) match {
      case None                               => Left(Errors.unknown(name))
      case Some(spec) if !arityOk(spec, args) => Left(Errors.arity(name))
      case Some(spec)                         =>
        spec.timeout.fold(Left(Errors.unknown(name))) { parse =>
          try Right(parse(args))
          catch { case reject: Reject => Left(reject.frame) }
        }
    }

  /**
    * Rewrites a blocking command's arguments once, before it starts waiting. `XREAD … $` uses this to fix `$` to the stream's last id at
    * the time of the call, as a server does, so that retries after a write still look for entries after that id.
    */
  def prepareBlocking(name: String, args: Vector[Bytes], now: Long): Vector[Bytes] = {
    val upper = name.toUpperCase(Locale.ROOT)
    specs.get(upper).flatMap(_.prepare) match {
      case None          => args
      case Some(prepare) =>
        locked {
          try prepare(new Call(upper, args, store, now))
          catch { case _: Reject => args }
        }
    }
  }

  /**
    * Runs `commands` in order at the same time `now`, without letting other commands run in between. `prepare` runs first under the same
    * lock and can stop the batch by returning `false`, which is how `EXEC` checks watched keys.
    */
  def executeAll(commands: Vector[(String, Vector[Bytes])], now: Long)(prepare: => Boolean): Option[Vector[Frame]] =
    locked(if (!prepare) None else Some(commands.map((name, args) => run(name, args, now, canBlock = false))))

  /**
    * The error a real server returns when a transaction queues this command, or `None` when the command would be queued. Redis rejects
    * unknown commands and wrong argument counts at queue time and then discards the whole transaction.
    */
  def queueError(name: String, args: Vector[Bytes]): Option[Frame] =
    specs.get(name.toUpperCase(Locale.ROOT)) match {
      case None                               => Some(Errors.unknown(name))
      case Some(spec) if !arityOk(spec, args) => Some(Errors.arity(name))
      case Some(_)                            => None
    }

  // blocking commands waiting for a write; guarded by the store lock
  private val waiters = mutable.ArrayBuffer.empty[() => Unit]

  /**
    * Runs `body` under the store lock. When `body` changed any key, wakes every waiting blocking command. Wake-ups, including pub/sub
    * deliveries, run after the lock is released, so that the code they resume does not run while the store is locked.
    */
  def locked[A](body: => A): A = {
    store.lock.lock()
    val before = store.changeCount
    try {
      val result = body
      if (store.changeCount != before && waiters.nonEmpty) {
        waiters.foreach(store.afterUnlock)
        waiters.clear()
      }
      result
    } finally {
      // taken even when body throws, so that a wake-up is never left behind for a later caller
      val actions = store.takeDeferred()
      store.lock.unlock()
      actions.foreach(_())
    }
  }

  private def run(name: String, args: Vector[Bytes], now: Long, canBlock: Boolean): Frame = {
    val upper = name.toUpperCase(Locale.ROOT)
    specs.get(upper) match {
      case None                               => Errors.unknown(name)
      case Some(spec) if !arityOk(spec, args) => Errors.arity(name)
      case Some(spec)                         =>
        try spec.run(new Call(upper, args, store, now))
        catch {
          case reject: Reject       => reject.frame
          case Blocked if !canBlock => Frame.Null
        }
    }
  }

  private def arityOk(spec: Spec, args: Vector[Bytes]): Boolean = {
    val count = args.length + 1
    if (spec.arity >= 0) count == spec.arity else count >= -spec.arity
  }
}
