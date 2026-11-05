package sage.client.inmemory

import java.util.Locale

import scala.collection.mutable
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.reflect.ClassTag

import sage.Bytes
import sage.codec.Doubles
import sage.protocol.Frame

/**
  * One command being executed: its name as the caller wrote it, its arguments, the store, and the time at which it runs. All commands in a
  * transaction see the same `now`, as they do on a real server. The parsing helpers throw [[Reject]] with the error a real server would
  * return.
  */
final private[inmemory] class Call(val name: String, val args: Vector[Bytes], val store: Store, val now: Long) {

  def key(index: Int): Key = new Key(args(index))

  def text(index: Int): String = args(index).asUtf8String

  def keyword(index: Int): String = text(index).toUpperCase(Locale.ROOT)

  def long(index: Int): Long = Numbers.parseLong(args(index)).getOrElse(throw new Reject(Errors.notInteger))

  // like a server, this fails with the same `error` for a malformed value and for a negative one
  inline def nonNegative(index: Int, inline error: Frame.SimpleError): Long =
    Numbers.parseLong(args(index)).filter(_ >= 0).getOrElse(throw new Reject(error))

  // reads the optional last argument at `index`; an argument after it fails with `extra`
  inline def optional[A](index: Int, inline extra: Frame.SimpleError = Errors.syntax)(inline read: Int => A): Option[A] =
    if (args.length > index + 1) throw new Reject(extra) else if (args.length > index) Some(read(index)) else None

  // true for `yes` and false for `no`; another word is a syntax error
  def choice(index: Int, yes: String, no: String): Boolean = {
    val word = keyword(index)
    if (word != yes && word != no) throw new Reject(Errors.syntax)
    word == yes
  }

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

  def unsupported(option: String): Nothing = throw new Reject(Errors.unsupported(option, name))

  // the position of the cursor that option parsers move through the arguments with `options`, `next` and `nextLong`
  private var at = 0

  // a missing option value is a syntax error, as on a server
  private def nextIndex(): Int = {
    if (at >= args.length) throw new Reject(Errors.syntax)
    at += 1
    at - 1
  }

  def next(): Bytes = args(nextIndex())

  def nextLong(): Long = long(nextIndex())

  // passes each upper-cased word from `from` to the end to `option`, which reads that option's values; inlined, so the loop's variables
  // stay local
  inline def options(from: Int)(inline option: String => Unit): Unit = {
    at = from
    while (at < args.length) option(keyword(nextIndex()))
  }

  def pairs(from: Int): Vector[(Key, Bytes)] = {
    if ((args.length - from) % 2 != 0) throw new Reject(Errors.arity(name))
    Vector.tabulate((args.length - from) / 2)(i => (key(from + 2 * i), args(from + 2 * i + 1)))
  }

  // most writes keep the key's expiry, as APPEND and INCR do on a real server
  def put(key: Key, value: Value, ttl: Ttl = Ttl.Keep): Unit = store.put(key, value, ttl, now)
}

/**
  * Describes one supported command. `arity` follows the convention of Redis `COMMAND INFO`: it counts the command name, a positive value
  * is an exact count, and a negative value is a minimum. A blocking command takes its timeout as its last argument.
  */
final private[inmemory] case class Spec(arity: Int, run: Call => Frame, blocking: Boolean = false)

private[inmemory] object Errors {

  def err(message: String): Frame.SimpleError = Frame.SimpleError(s"ERR $message")

  val syntax: Frame.SimpleError     = err("syntax error")
  val notInteger: Frame.SimpleError = err("value is not an integer or out of range")
  val notFloat: Frame.SimpleError   = err("value is not a valid float")
  val noSuchKey: Frame.SimpleError  = err("no such key")
  val outOfRange: Frame.SimpleError = err("value is out of range, must be positive")
  val wrongType: Frame.SimpleError  = Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")

  def arity(name: String): Frame.SimpleError = err(s"wrong number of arguments for '${name.toLowerCase(Locale.ROOT)}' command")

  def invalidExpire(name: String): Frame.SimpleError = err(s"invalid expire time in '${name.toLowerCase(Locale.ROOT)}' command")

  def unknown(name: String): Frame.SimpleError = err(s"unknown command '$name': the in-memory client does not support it")

  def unsupported(option: String, name: String): Frame.SimpleError =
    err(s"the in-memory client does not support the $option option of '${name.toUpperCase(Locale.ROOT)}'")
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

  // no collection here holds more than Int.MaxValue items, so a larger count takes all of them
  def size(count: Long): Int = math.min(count, Int.MaxValue.toLong).toInt

  inline def added(current: Option[Bytes], amount: Long, inline invalid: Frame.SimpleError): Long =
    try Math.addExact(current.fold(0L)(parseLong(_).getOrElse(throw new Reject(invalid))), amount)
    catch { case _: ArithmeticException => throw new Reject(Errors.err("increment or decrement would overflow")) }

  inline def addedFloat(current: Option[Bytes], amount: Double, inline invalid: Frame.SimpleError): Bytes = {
    val updated = current.fold(0.0)(parseDouble(_).getOrElse(throw new Reject(invalid))) + amount
    if (updated.isNaN || updated.isInfinite) throw new Reject(Errors.err("increment would produce NaN or Infinity"))
    Bytes.utf8(formatDouble(updated))
  }

  /**
    * Formats a double the way `INCRBYFLOAT` does: plain decimal notation with no exponent and no trailing zeros.
    */
  def formatDouble(value: Double): String = {
    val plain = new java.math.BigDecimal(value.toString).stripTrailingZeros.toPlainString
    if (plain == "-0") "0" else plain
  }
}

/**
  * Runs commands against its own [[Store]]. Each call to [[execute]] or [[executeAll]] holds the store lock for its whole duration.
  */
final private[sage] class Executor {

  val store: Store = new Store

  private val specs: Map[String, Spec] =
    StringCommands.specs ++ KeyCommands.specs ++ ServerCommands.specs ++ HashCommands.specs ++ ListCommands.specs ++ SetCommands.specs ++
      SortedSetCommands.specs ++ PubSubCommands.specs

  /**
    * Runs one command at `now`, a Unix time in milliseconds.
    */
  def execute(name: String, args: Vector[Bytes], now: Long): Frame = locked(run(name, args, now))

  /**
    * Runs one attempt of a blocking command at `now`. A blocking command waits exactly when it would reply with null, as on a server. A
    * [[Waiter]] runs it under the store lock.
    */
  def attemptBlocking(name: String, args: Vector[Bytes])(now: Long): Option[Frame] =
    run(name, args, now) match {
      case Frame.Null => None
      case frame      => Some(frame)
    }

  /**
    * A blocking command or lock that runs `attempt` until it returns a value. Its fields are guarded by the store lock. After [[abandon]],
    * `attempt` never runs again and the wait never resumes, so a wait that its timeout could not interrupt, as on Pekko, cannot act later.
    */
  final class Waiter[A](attempt: Long => Option[A]) {
    private var result: Option[A] = None
    private var abandoned         = false

    /**
      * Runs `attempt` at `now`. When it returns `None`, this registers `onChange` to be called after the next write. Both happen under the
      * store lock, so a write cannot slip in between and be missed.
      */
    def poll(now: Long, onChange: () => Unit): Option[A] =
      locked {
        if (!abandoned) {
          result = attempt(now)
          if (result.isEmpty) waiters += onChange
        }
        result
      }

    // returns the value that an attempt took before the caller gave up, so that it is not lost
    def abandon(): Option[A] =
      locked {
        abandoned = true
        result
      }
  }

  /**
    * Parses the timeout of a blocking command once, before it starts waiting, or returns the error frame for an invalid command or
    * timeout. The timeout is in seconds, possibly fractional, and `None` means waiting forever, which a timeout of 0 requests.
    */
  def startBlocking(name: String, args: Vector[Bytes]): Either[Frame, Option[FiniteDuration]] =
    try {
      if (!spec(name, args).blocking) throw new Reject(Errors.unknown(name))
      val seconds = Numbers.parseDouble(args.last).filter(_ >= 0).getOrElse(throw new Reject(Errors.err("timeout is not a float or out of range")))
      // a FiniteDuration stops at 292 years
      val ms      = math.min(math.ceil(seconds * 1000).toLong, Long.MaxValue / 1000000L)
      Right(Option.when(ms > 0)(ms.millis))
    } catch { case reject: Reject => Left(reject.frame) }

  /**
    * Runs the commands of an `EXEC` in order at the same time `now`, without letting other commands run in between. `prepare` runs first
    * under the same lock and can stop the batch by returning `false`, which is how `EXEC` checks watched keys. Like a server, this discards
    * the whole batch with the error of the first unknown command or wrong argument count, which Redis rejects when the command is queued.
    */
  def executeAll(commands: Vector[(String, Vector[Bytes])], now: Long)(prepare: => Boolean): Either[String, Option[Vector[Frame]]] =
    locked {
      val proceed = prepare
      try {
        val calls = commands.map((name, args) => (spec(name, args), new Call(name, args, store, now)))
        Right(Option.when(proceed)(calls.map(attempt(_, _))))
      } catch { case reject: Reject => Left(reject.frame.value) }
    }

  // callers waiting for a write; guarded by the store lock
  private val waiters = mutable.ArrayBuffer.empty[() => Unit]

  // lets tests wait until a blocking command is registered
  private[sage] def waiting: Int = locked(waiters.length)

  /**
    * Runs `body` under the store lock. When `body` changed any key, wakes every blocking command and lock that waits for a write. Wake-ups,
    * including pub/sub deliveries, run after the lock is released, so that the code they resume does not run while the store is locked.
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

  private def run(name: String, args: Vector[Bytes], now: Long): Frame =
    try spec(name, args).run(new Call(name, args, store, now))
    catch { case reject: Reject => reject.frame }

  private def attempt(spec: Spec, call: Call): Frame =
    try spec.run(call)
    catch { case reject: Reject => reject.frame }

  private def spec(name: String, args: Vector[Bytes]): Spec =
    specs.get(name.toUpperCase(Locale.ROOT)) match {
      case Some(spec) if arityOk(spec, args) => spec
      case Some(_)                           => throw new Reject(Errors.arity(name))
      case None                              => throw new Reject(Errors.unknown(name))
    }

  private def arityOk(spec: Spec, args: Vector[Bytes]): Boolean = {
    val count = args.length + 1
    if (spec.arity >= 0) count == spec.arity else count >= -spec.arity
  }
}
