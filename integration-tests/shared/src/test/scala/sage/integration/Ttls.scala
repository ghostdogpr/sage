package sage.integration

import scala.concurrent.duration.*

import sage.commands.{FieldTtl, Ttl}

/**
  * Ttl inspection the command suites share: a server-reported expiry is only ever asserted within a bound.
  */
object Ttls {

  private def remaining(ttl: Ttl | FieldTtl): Option[FiniteDuration] =
    ttl match {
      case Ttl.Expires(value)      => Some(value)
      case FieldTtl.Expires(value) => Some(value)
      case _                       => None
    }

  def expiresWithin(ttl: Ttl | FieldTtl, bound: FiniteDuration, above: FiniteDuration = Duration.Zero): Boolean =
    remaining(ttl).exists(value => value > above && value <= bound)

  def renewed(before: Ttl, after: Ttl): Boolean =
    remaining(before).zip(remaining(after)).exists((previous, current) => current > previous + 100.millis)
}
