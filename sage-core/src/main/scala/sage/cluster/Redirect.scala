package sage.cluster

/**
  * The two cluster redirect types. `Moved` means the slot has a new owner and the topology should be refreshed. `Ask` is temporary during
  * a slot migration: send this command to the named node with an `ASKING` prefix, but do not update the topology.
  */
private[sage] enum RedirectKind {
  case Moved, Ask
}

/**
  * A parsed `MOVED`/`ASK` reply.
  */
final private[sage] case class Redirect(kind: RedirectKind, private val announced: Node) {

  // an empty announced host means the node that sent the redirect (e.g. `MOVED 3999 :6381`)
  def target(from: Node): Node = if (announced.host.isEmpty) Node(from.host, announced.port) else announced
}

private[sage] object Redirect {

  def parse(kind: RedirectKind, detail: String): Option[Redirect] =
    detail.split(' ') match {
      case Array(_, address) => addressOf(address).map(Redirect(kind, _))
      case _                 => None
    }

  private def addressOf(address: String): Option[Node] = {
    val colon = address.lastIndexOf(':') // last, so IPv6 hosts keep their colons; an empty host is preserved
    if (colon < 0) None
    else address.substring(colon + 1).toIntOption.filter(port => port >= 1 && port <= 65535).map(Node(address.substring(0, colon), _))
  }
}
