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
    * Decodes the elements of an invalidation push frame. Returns `None` for malformed frames and other push kinds. A null key list becomes
    * [[FlushAll]]. Pub/sub push frames are handled separately by [[Pubsub.decode]].
    */
  def decode(elements: Vector[Frame]): Option[Invalidation] =
    elements match {
      case Vector(kind, keys) if isInvalidate(kind) =>
        keys match {
          case Frame.Null            => Some(FlushAll)
          case Frame.Array(elements) =>
            val builder = Vector.newBuilder[Bytes]
            val it      = elements.iterator
            while (it.hasNext)
              it.next() match {
                case Frame.BulkString(key) => builder += key
                case _                     => return None
              }
            Some(Evict(builder.result()))
          case _                     => None
        }
      case _                                        => None
    }

  private def isInvalidate(frame: Frame): Boolean =
    frame match {
      case Frame.BulkString(b)   => b.asUtf8String == "invalidate"
      case Frame.SimpleString(s) => s == "invalidate"
      case _                     => false
    }
}
