package sage.client.internal

import sage.SageException.{ConnectionLost, NotConnected, ServerError}
import sage.cluster.{Redirect, RedirectKind}

/**
  * The shared classification of a failed command. Both runtimes interpret a [[Throwable]] through this type and therefore use the same rules.
  */
private[client] enum Fault {
  case Redirected(redirect: Redirect)
  case Demoted
  case Lost(mayHaveExecuted: Boolean)
  case TryAgain
  case Unavailable(clusterWide: Boolean)
  case Fatal

  def refreshPolicy: RefreshPolicy = this match {
    case Fatal | TryAgain                                          => RefreshPolicy.Skip
    // only a cluster-wide refusal implies the mapping moved
    case Unavailable(clusterWide)                                  => if (clusterWide) RefreshPolicy.Forced else RefreshPolicy.Skip
    // ASK does not change ownership, so discovery cannot find a new owner until the migration finishes
    case Redirected(redirect) if redirect.kind == RedirectKind.Ask => RefreshPolicy.Throttled
    case Redirected(_) | Demoted | Lost(_)                         => RefreshPolicy.Forced
  }

  // return true when the server rejected the command before execution and retrying the same command is safe
  def selfClearing: Boolean = this match {
    case TryAgain | Unavailable(_) => true
    case _                         => false
  }
}

/**
  * Controls topology refresh after a [[Fault]]. `Skip` does not refresh. `Throttled` respects `minRefreshInterval`, and `Forced` refreshes
  * immediately.
  */
private[client] enum RefreshPolicy {
  case Skip, Throttled, Forced
}

private[client] object Fault {

  def categorize(error: Throwable): Fault =
    error match {
      case ServerError("MOVED", detail)             => redirected(RedirectKind.Moved, detail)
      case ServerError("ASK", detail)               => redirected(RedirectKind.Ask, detail)
      case ServerError("READONLY", _)               => Fault.Demoted
      case ServerError("TRYAGAIN", _)               => Fault.TryAgain
      case ServerError("CLUSTERDOWN", _)            => Fault.Unavailable(clusterWide = true)
      case ServerError("LOADING" | "MASTERDOWN", _) => Fault.Unavailable(clusterWide = false)
      case NotConnected()                           => Fault.Lost(mayHaveExecuted = false)
      case ConnectionLost(executed)                 => Fault.Lost(executed)
      case _                                        => Fault.Fatal
    }

  private def redirected(kind: RedirectKind, detail: String): Fault =
    Redirect.parse(kind, detail) match {
      case Some(redirect) => Fault.Redirected(redirect)
      case None           => Fault.Fatal
    }
}
