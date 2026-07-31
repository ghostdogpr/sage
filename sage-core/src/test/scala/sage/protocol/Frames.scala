package sage.protocol

import sage.Bytes

/**
  * Frame constructors the specs share.
  */
object Frames {

  def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  def map(entries: (String, Frame)*): Frame = Frame.Map(entries.toVector.map { case (key, value) => bulk(key) -> value })
}
