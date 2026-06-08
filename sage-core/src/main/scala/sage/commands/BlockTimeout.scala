package sage.commands

import scala.concurrent.duration.FiniteDuration

import sage.Bytes

/**
  * How long a blocking command waits server-side. `Forever` is the wire `0` (block until data) — named so it cannot be confused with a
  * zero duration, which would read as "do not wait". A cross-family primitive shared identically by every blocking command (`BLPOP`,
  * `BZPOPMIN`, `XREAD BLOCK`, …), like [[ListSide]].
  */
enum BlockTimeout {
  case Forever
  case After(duration: FiniteDuration)
}

object BlockTimeout {

  // sub-second timeouts use the decimal-seconds wire form, rounding up so the wait is never shorter than asked. `After` floors at one
  // millisecond: a zero or negative duration must never emit "0", which is the wire's "block forever" — only `Forever` may mean infinite.
  private[commands] def wire(timeout: BlockTimeout): Bytes =
    timeout match {
      case Forever         => Zero
      case After(duration) =>
        val millis = Math.max(1L, Math.ceilDiv(duration.toNanos, 1000000L))
        val text   =
          if (millis % 1000L == 0L) (millis / 1000L).toString
          else java.math.BigDecimal.valueOf(millis, 3).stripTrailingZeros.toPlainString
        Bytes.utf8(text)
    }

  private val Zero = Bytes.utf8("0")
}
