package sage.commands

import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.protocol.Frame

/**
  * Helpers that decode a raw [[sage.protocol.Frame]] returned by `eval` or `fcall`. User code determines the reply shape. The
  * `sage.commands.*` import provides calls such as `run(eval(...)).map(_.asLong)`. These helpers use the same strict decoders as typed
  * commands. Each one returns `Either[sage.SageException.DecodeError, _]` and does not throw.
  */
extension (frame: Frame) {

  /**
    * Decodes the whole frame as a single value of `A` through its [[sage.codec.ValueCodec]].
    */
  def as[A](using ValueCodec[A]): Either[DecodeError, A] = Decode.value[A].apply(frame)

  /**
    * Decodes an array/set/push frame into a `Vector[A]`, decoding each element through its [[sage.codec.ValueCodec]].
    */
  def asArrayOf[A](using ValueCodec[A]): Either[DecodeError, Vector[A]] = Decode.vector(Decode.value[A]).apply(frame)

  /**
    * The raw, undecoded elements of an array/set/push frame, for replies whose elements are heterogeneous.
    */
  def asArray: Either[DecodeError, Vector[Frame]] =
    frame match {
      case Frame.Array(elements) => Right(elements)
      case Frame.Set(elements)   => Right(elements)
      case Frame.Push(elements)  => Right(elements)
      case other                 => Left(DecodeError("array", Frame.describe(other)))
    }

  /**
    * Decodes an integer frame as a `Long`.
    */
  def asLong: Either[DecodeError, Long] = Decode.long(frame)

  /**
    * Decodes a string frame as a `String`, however the server framed it: simple, bulk, or verbatim.
    */
  def asString: Either[DecodeError, String] = Decode.text(frame)
}
