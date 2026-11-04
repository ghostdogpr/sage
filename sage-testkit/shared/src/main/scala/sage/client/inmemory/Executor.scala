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

  def negatable(index: Int): Long = bounded(index, Errors.notInteger)(_ != Long.MinValue)

  // an integer that `valid` accepts; like a server, this fails with the same `error` for a malformed value and for an invalid one
  inline def bounded(index: Int, inline error: Frame.SimpleError)(valid: Long => Boolean): Long =
    Numbers.parseLong(args(index)).filter(valid).getOrElse(throw new Reject(error))

  // reads the optional last argument at `index`; an argument after it fails with `extra`
  inline def optional[A](index: Int, inline extra: Frame.SimpleError = Errors.syntax)(inline read: Int => A): Option[A] =
    if (args.length > index + 1) throw new Reject(extra) else if (args.length > index) Some(read(index)) else None

  // whether the optional last argument at `index` is `word`; another word is a syntax error
  inline def optionalWord(index: Int, word: String, inline extra: Frame.SimpleError = Errors.syntax): Boolean =
    optional(index, extra)(i => keyword(i) == word || (throw new Reject(Errors.syntax))).isDefined

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

  // the position of the cursor that option parsers move through the arguments with `next` and the methods below
  private var at = 0

  def seek(index: Int): Unit = at = index

  def position: Int = at

  def hasNext: Boolean = at < args.length

  def remaining: Int = args.length - at

  // a missing option value is a syntax error, as on a server
  def need(count: Int): Unit = if (remaining < count) throw new Reject(Errors.syntax)

  def nextIndex(): Int = {
    need(1)
    at += 1
    at - 1
  }

  def next(): Bytes = args(nextIndex())

  def nextKeyword(): String = keyword(nextIndex())

  def nextLong(): Long = long(nextIndex())

  inline def nextBounded(inline error: Frame.SimpleError)(valid: Long => Boolean): Long = bounded(nextIndex(), error)(valid)

  def nextDouble(): Double = double(nextIndex())

  def nextKey(): Key = key(nextIndex())

  // passes each upper-cased word from `from` to the end to `option`, which reads that option's values; inlined, so the loop's variables
  // stay local
  inline def options(from: Int)(inline option: String => Unit): Unit = {
    seek(from)
    while (hasNext) option(nextKeyword())
  }

  // consumes the next argument when it is the optional `word`
  def nextIs(word: String): Boolean = hasNext && keyword(at) == word && { at += 1; true }

  /**
    * Reads the `count` keys after the numkeys argument at `index`, leaving the cursor after them.
    */
  inline def keys(index: Int, count: Long, inline tooMany: Frame.SimpleError = Errors.syntax): Vector[Key] = {
    if (count > args.length - index - 1) throw new Reject(tooMany)
    seek(index + 1 + count.toInt)
    Vector.tabulate(count.toInt)(i => key(index + 1 + i))
  }

  inline def numKeys(index: Int, inline tooMany: Frame.SimpleError = Errors.syntax): Vector[Key] =
    keys(index, bounded(index, Errors.err("numkeys should be greater than 0"))(_ > 0), tooMany)

  def pairs(from: Int): Vector[(Key, Bytes)] = {
    if ((args.length - from) % 2 != 0) throw new Reject(Errors.arity(name))
    Vector.tabulate((args.length - from) / 2)(i => (key(from + 2 * i), args(from + 2 * i + 1)))
  }

  // the index of the value of an optional `word n` that ends the arguments at the cursor, as in `LMPOP … COUNT n`
  def trailing(word: String): Option[Int] =
    remaining match {
      case 0                        => None
      case 2 if keyword(at) == word => Some(at + 1)
      case _                        => throw new Reject(Errors.syntax)
    }

  // most writes keep the key's expiry, as APPEND, INCR and SETRANGE do on a real server
  def put(key: Key, value: Value, ttl: Ttl = Ttl.Keep, watched: Boolean = true): Unit = store.put(key, value, ttl, now, watched)

  def stored(destination: Key, value: Value, size: Int): Frame = {
    put(destination, value, Ttl.Clear)
    Frame.Integer(size.toLong)
  }
}

/**
  * Describes one supported command. `arity` follows the convention of Redis `COMMAND INFO`: it counts the command name, a positive value
  * is an exact count, and a negative value is a minimum.
  */
final private[inmemory] case class Spec(arity: Int, run: Call => Frame, blocking: Option[Call => Wait] = None)

private[inmemory] object Spec {

  // a server rejects an invalid timeout even where the command cannot block, as inside `EXEC`
  def blocking(arity: Int, run: Call => Frame)(timeout: Call => Option[FiniteDuration]): Spec =
    Spec(arity, call => Wait(timeout(call), run).attempt(call), Some(call => Wait(timeout(call), run)))
}

/**
  * A blocking command after its arguments were parsed: its timeout, `None` to wait forever, and the attempt that each retry repeats with
  * the retry's time. XREAD fixes `$` to the stream's last id when it builds its attempt, as a server does, so that retries after a write
  * still look for entries after that id.
  */
final private[inmemory] case class Wait(timeout: Option[FiniteDuration], attempt: Call => Frame)

/**
  * Parses the timeout of a blocking command. `None` means waiting forever, which a timeout of 0 requests. An invalid timeout throws
  * [[Reject]] with the server's error.
  */
private[inmemory] object Timeouts {

  // BLPOP, BLMOVE, BZPOPMIN and the other list and sorted-set forms take seconds, possibly fractional
  def seconds(arg: Bytes, now: Long): Option[FiniteDuration] =
    Numbers.parseDouble(arg) match {
      case None                         => throw new Reject(Errors.err("timeout is not a float or out of range"))
      case Some(seconds) if seconds < 0 => throw new Reject(Errors.err("timeout is negative"))
      case Some(seconds)                => waiting(math.ceil(seconds * 1000).toLong, now)
    }

  // XREAD and XREADGROUP take whole milliseconds after BLOCK
  def millis(arg: Bytes, now: Long): Option[FiniteDuration] =
    Numbers.parseLong(arg) match {
      case None               => throw new Reject(Errors.err("timeout is not an integer or out of range"))
      case Some(ms) if ms < 0 => throw new Reject(Errors.err("timeout is negative"))
      case Some(ms)           => waiting(ms, now)
    }

  // `toLong` saturates huge seconds to Long.MaxValue, which a server refuses like any deadline past it; a FiniteDuration stops at 292 years
  private def waiting(ms: Long, now: Long): Option[FiniteDuration] =
    if (ms == Long.MaxValue || now > Long.MaxValue - ms) throw new Reject(Errors.err("timeout is out of range"))
    else Option.when(ms > 0)(math.min(ms, Long.MaxValue / 1000000L).millis)
}

private[inmemory] object Errors {

  def err(message: String): Frame.SimpleError = Frame.SimpleError(s"ERR $message")

  val syntax: Frame.SimpleError     = err("syntax error")
  val notInteger: Frame.SimpleError = err("value is not an integer or out of range")
  val notFloat: Frame.SimpleError   = err("value is not a valid float")
  val noSuchKey: Frame.SimpleError  = err("no such key")
  val outOfRange: Frame.SimpleError = err("value is out of range, must be positive")
  val wrongType: Frame.SimpleError  = Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")

  def arity(name: String): Frame.SimpleError = err(s"wrong number of arguments for '${name.toLowerCase(Locale.ROOT).replace(' ', '|')}' command")

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
      SortedSetCommands.specs ++ Scripts.specs ++ PubSubCommands.specs ++ StreamCommands.specs

  /**
    * Runs one command at `now`, a Unix time in milliseconds.
    */
  def execute(name: String, args: Vector[Bytes], now: Long): Frame = locked(run(name, args, now))

  /**
    * Runs one attempt of a blocking command at `now`. A blocking command waits exactly when it would reply with null, as on a server, and
    * replies with null at once elsewhere, for example inside `EXEC`. When it has nothing to return yet, this registers `onChange` to be
    * called after the next write and returns `None`. Both happen under the store lock, so a write cannot slip in between and be missed.
    */
  def attemptBlocking(name: String, args: Vector[Bytes], wait: Wait, now: Long, onChange: () => Unit): Option[Frame] =
    locked {
      val call = new Call(name, args, store, now)
      (try wait.attempt(call)
      catch { case reject: Reject => reject.frame }) match {
        case Frame.Null =>
          waiters += onChange
          None
        case frame      => Some(frame)
      }
    }

  /**
    * Parses a blocking command once, before it starts waiting, or returns the error frame for an invalid command or timeout.
    */
  def startBlocking(name: String, args: Vector[Bytes], now: Long): Either[Frame, Wait] =
    locked {
      try Right(resolve(name, args, now)((spec, call) => spec.blocking.fold(throw new Reject(Errors.unknown(call.name)))(_(call))))
      catch { case reject: Reject => Left(reject.frame) }
    }

  /**
    * Runs the commands of an `EXEC` in order at the same time `now`, without letting other commands run in between. `prepare` runs first
    * under the same lock and can stop the batch by returning `false`, which is how `EXEC` checks watched keys. Like a server, this discards
    * the whole batch with the error of the first unknown command or wrong argument count, which Redis rejects when the command is queued.
    */
  def executeAll(commands: Vector[(String, Vector[Bytes])], now: Long)(prepare: => Boolean): Either[String, Option[Vector[Frame]]] =
    locked {
      val proceed = prepare
      try {
        val calls = commands.map((name, args) => resolve(name, args, now)((spec, call) => (spec, call)))
        Right(Option.when(proceed)(calls.map(attempt(_, _))))
      } catch { case reject: Reject => Left(reject.frame.value) }
    }

  // blocking commands waiting for a write; guarded by the store lock
  private val waiters = mutable.ArrayBuffer.empty[() => Unit]

  // lets tests wait until a blocking command is registered
  private[sage] def waiting: Int = locked(waiters.length)

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

  private def run(name: String, args: Vector[Bytes], now: Long): Frame =
    try resolve(name, args, now)((spec, call) => spec.run(call))
    catch { case reject: Reject => reject.frame }

  private def attempt(spec: Spec, call: Call): Frame =
    try spec.run(call)
    catch { case reject: Reject => reject.frame }

  // the commands such as XINFO whose first argument names a subcommand
  private val containers: Set[String] = specs.keySet.filter(_.contains(' ')).map(_.takeWhile(_ != ' '))

  /**
    * Passes the spec of the command `name` and the [[Call]] that runs it to `use`. Sage's typed API puts the subcommand of a command such
    * as `XINFO` in `name`, and a raw command sends it as the first argument, so both forms find the same spec.
    */
  private def resolve[A](name: String, args: Vector[Bytes], now: Long)(use: (Spec, Call) => A): A = {
    val upper = name.toUpperCase(Locale.ROOT)
    specs.get(upper) match {
      case Some(spec) if arityOk(spec, args)          => use(spec, new Call(name, args, store, now))
      case Some(_)                                    => throw new Reject(Errors.arity(name))
      case None if containers(upper) && args.nonEmpty => resolve(s"$name ${args(0).asUtf8String}", args.drop(1), now)(use)
      case None if containers(upper)                  => throw new Reject(Errors.arity(name))
      case None                                       => throw new Reject(Errors.unknown(name))
    }
  }

  private def arityOk(spec: Spec, args: Vector[Bytes]): Boolean = {
    val count = args.length + 1
    if (spec.arity >= 0) count == spec.arity else count >= -spec.arity
  }
}
