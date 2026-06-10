package sage.commands

import sage.Bytes
import sage.protocol.Frame

/**
  * A classified client-side-cache invalidation push, delivered on the Multiplexed Connection (not the Subscription Connection). `Evict`
  * names the keys the server says changed; `FlushAll` is the null-payload form the server sends on `FLUSHALL`/`FLUSHDB` or when it drops
  * tracking state, meaning the whole local cache is now untrustworthy.
  */
enum Invalidation {
  case Evict(keys: Vector[Bytes])
  case FlushAll
}

object Invalidation {

  /**
    * Classifies a push frame's elements. `None` for any push that is not an `invalidate` message (so it sits beside [[Pubsub.decode]],
    * which returns `None` for these). A null key list decodes to [[FlushAll]].
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
