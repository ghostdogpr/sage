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
      case e: ServerError           =>
        Redirect.parse(e.getMessage) match {
          case Some(redirect)                                        => Fault.Redirected(redirect)
          case None if e.code == "READONLY"                          => Fault.Demoted
          case None if e.code == "TRYAGAIN"                          => Fault.TryAgain
          case None if e.code == "CLUSTERDOWN"                       => Fault.Unavailable(clusterWide = true)
          case None if e.code == "LOADING" || e.code == "MASTERDOWN" => Fault.Unavailable(clusterWide = false)
          case None                                                  => Fault.Fatal
        }
      case NotConnected()           => Fault.Lost(mayHaveExecuted = false)
      case ConnectionLost(executed) => Fault.Lost(executed)
      case _                        => Fault.Fatal
    }
}
