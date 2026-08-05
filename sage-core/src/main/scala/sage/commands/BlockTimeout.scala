package sage.commands

import scala.concurrent.duration.FiniteDuration

import sage.Bytes

/**
  * How long a blocking command waits on the server. `Forever` sends `0`, which means to wait until data is available. It is separate from
  * a zero duration, which would normally mean not to wait. This type is shared by blocking commands such as `BLPOP`, `BZPOPMIN`, and
  * `XREAD BLOCK`.
  */
enum BlockTimeout {
  case Forever
  case After(duration: FiniteDuration)
}

object BlockTimeout {

  // Commands such as BLPOP, BLMOVE, and BZPOPMIN express timeouts in seconds. Round sub-second values up to the next millisecond so the
  // server does not wait for less time than requested. Use at least one millisecond because the wire value 0 means to wait forever.
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

  // the millisecond form `XREAD`/`XREADGROUP` `BLOCK` requires (the SECONDS [[wire]] form would silently shorten the wait 1000x)
  private[commands] def millisWire(timeout: BlockTimeout): Bytes =
    timeout match {
      case Forever         => Zero
      case After(duration) => Bytes.utf8(Math.max(1L, Math.ceilDiv(duration.toNanos, 1000000L)).toString)
    }

  private val Zero = Bytes.utf8("0")
}
