package sage.client.internal

import sage.SageException.{ConnectionLost, NotConnected, ServerError}
import sage.cluster.{Redirect, RedirectKind}

/**
  * The shared categorization of a failed command: both runtimes read a [[Throwable]] the same way, so they cannot drift on what a fault is.
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
    // only a cluster-wide refusal implies the slot mapping moved; a node-local one leaves it valid
    case Unavailable(clusterWide)                                  => if (clusterWide) RefreshPolicy.Forced else RefreshPolicy.Skip
    // ASK moves no ownership: discovery has nothing to adopt until the migration finalizes
    case Redirected(redirect) if redirect.kind == RedirectKind.Ask => RefreshPolicy.Throttled
    case Redirected(_) | Demoted | Lost(_)                         => RefreshPolicy.Forced
  }

  // the server refused before running the command, for a reason that clears on its own, so the same command is safe to send again
  def selfClearing: Boolean = this match {
    case TryAgain | Unavailable(_) => true
    case _                         => false
  }
}

/**
  * What a [[Fault]] asks of topology discovery, weakest first: no refresh, one inside the `minRefreshInterval` window, or one that bypasses it.
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
          // the server refused before running the command and the refusal clears on its own; only CLUSTERDOWN implies the slot map moved
          case None if e.code == "CLUSTERDOWN"                       => Fault.Unavailable(clusterWide = true)
          case None if e.code == "LOADING" || e.code == "MASTERDOWN" => Fault.Unavailable(clusterWide = false)
          case None                                                  => Fault.Fatal
        }
      case NotConnected()           => Fault.Lost(mayHaveExecuted = false)
      case ConnectionLost(executed) => Fault.Lost(executed)
      case _                        => Fault.Fatal
    }
}
