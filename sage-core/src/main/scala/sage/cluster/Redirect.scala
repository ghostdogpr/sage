package sage.cluster

/**
  * `Moved` is permanent — the slot's owner changed, refresh the topology — while `Ask` is a one-shot hand-off during a live migration:
  * send the single command (prefixed `ASKING`) to the named node without touching the topology.
  */
private[sage] enum RedirectKind {
  case Moved, Ask
}

/**
  * A parsed `MOVED`/`ASK` reply. An empty target host means the IP of the current connection, which the runtime substitutes.
  */
final private[sage] case class Redirect(kind: RedirectKind, slot: Slot, target: Node)

private[sage] object Redirect {

  def parse(error: String): Option[Redirect] = {
    val parts = error.split(' ')
    if (parts.length == 3)
      for {
        kind   <- kindOf(parts(0))
        slot   <- parts(1).toIntOption.flatMap(Slot.at)
        target <- addressOf(parts(2))
      } yield Redirect(kind, slot, target)
    else None
  }

  private def kindOf(token: String): Option[RedirectKind] =
    token match {
      case "MOVED" => Some(RedirectKind.Moved)
      case "ASK"   => Some(RedirectKind.Ask)
      case _       => None
    }

  private def addressOf(address: String): Option[Node] = {
    val colon = address.lastIndexOf(':') // last, so IPv6 hosts keep their colons; an empty host is preserved
    if (colon < 0) None
    else address.substring(colon + 1).toIntOption.filter(port => port >= 1 && port <= 65535).map(Node(address.substring(0, colon), _))
  }
}
