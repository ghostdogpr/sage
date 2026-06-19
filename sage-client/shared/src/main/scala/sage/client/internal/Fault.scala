package sage.client.internal

import sage.SageException.{ConnectionLost, NotConnected, ServerError}
import sage.cluster.Redirect

/**
  * The shared categorization of a failed command: both runtimes read a [[Throwable]] the same way, so they cannot drift on what a fault is.
  */
private[client] enum Fault {
  case Redirected(redirect: Redirect)
  case Demoted
  case Lost(mayHaveExecuted: Boolean)
  case Fatal
}

private[client] object Fault {

  def categorize(error: Throwable): Fault =
    error match {
      case e: ServerError           =>
        Redirect.parse(e.getMessage) match {
          case Some(redirect)               => Fault.Redirected(redirect)
          case None if e.code == "READONLY" => Fault.Demoted
          case None                         => Fault.Fatal
        }
      case NotConnected()           => Fault.Lost(mayHaveExecuted = false)
      case ConnectionLost(executed) => Fault.Lost(executed)
      case _                        => Fault.Fatal
    }
}
