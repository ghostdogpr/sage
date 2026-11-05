package sage.protocol

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Frame constructors the specs share.
  */
object Frames {

  def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  extension (parser: RespParser) {
    def feed(bytes: Bytes): Either[ProtocolError, Vector[Frame]] = {
      val frames = Vector.newBuilder[Frame]
      val array  = bytes.unsafeArray
      parser.feed(array, 0, array.length)(frames += _).toLeft(frames.result())
    }
  }

  def map(entries: (String, Frame)*): Frame = Frame.Map(entries.toVector.map { case (key, value) => bulk(key) -> value })

  // parses one raw reply, so a spec can decode the exact bytes captured from Redis or Valkey
  def wire(raw: String): Frame =
    new RespParser().feed(Bytes.utf8(raw)) match {
      case Right(Vector(frame)) => frame
      case other                => throw new IllegalArgumentException(s"expected exactly one frame, got $other")
    }
}
