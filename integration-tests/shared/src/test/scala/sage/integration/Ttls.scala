package sage.integration

import scala.concurrent.duration.*

import sage.commands.{FieldTtl, Ttl}

/**
  * Ttl inspection the command suites share: a server-reported expiry is only ever asserted within a bound.
  */
object Ttls {

  def remaining(ttl: Ttl): Option[FiniteDuration] =
    ttl match {
      case Ttl.Expires(value) => Some(value)
      case _                  => None
    }

  private def remaining(ttl: FieldTtl): Option[FiniteDuration] =
    ttl match {
      case FieldTtl.Expires(value) => Some(value)
      case _                       => None
    }

  def expires(ttl: Ttl): Boolean = remaining(ttl).exists(_ > Duration.Zero)

  def expires(ttl: FieldTtl): Boolean = remaining(ttl).exists(_ > Duration.Zero)

  def expiresWithin(ttl: Ttl, bound: FiniteDuration): Boolean = remaining(ttl).exists(value => value > Duration.Zero && value <= bound)

  def expiresWithin(ttl: FieldTtl, bound: FiniteDuration): Boolean = remaining(ttl).exists(value => value > Duration.Zero && value <= bound)
}
