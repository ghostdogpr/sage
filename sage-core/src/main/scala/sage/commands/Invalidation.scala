package sage.commands

import sage.Bytes
import sage.protocol.Frame

/**
  * A client-side cache invalidation received by the multiplexed connection. `Evict` contains the keys reported as changed. `FlushAll`
  * means the entire local cache must be cleared; the server sends it after `FLUSHALL`, `FLUSHDB`, or loss of its tracking state.
  */
private[sage] enum Invalidation {
  case Evict(keys: Vector[Bytes])
  case FlushAll
}

private[sage] object Invalidation {

  /**
    * Decodes the elements of an invalidation push frame. Returns `None` for other push kinds. A null or undecodable key list becomes
    * [[FlushAll]], so no stale entry survives. Pub/sub push frames are handled separately by [[Pubsub.decode]].
    */
  def decode(elements: Vector[Frame]): Option[Invalidation] =
    elements match {
      case Vector(Decode.Text("invalidate"), keys) => Some(evictedKeys(keys).fold(_ => FlushAll, Evict(_)))
      case _                                       => None
    }

  private val evictedKeys = Decode.vector(Decode.bytes)
}
