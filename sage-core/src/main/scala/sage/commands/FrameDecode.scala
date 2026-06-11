package sage.commands

import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.protocol.Frame

/**
  * Decode helpers for a raw [[Frame]] — the result type of `eval`/`fcall` (ADR-0033), whose reply shape is defined by user code. Imported
  * with `sage.commands.*`, they turn the "caller `.map`s a decoder" contract into one-liners (`run(eval(...)).map(_.asLong)`), reusing the
  * same strict decoders the typed commands use. Each returns `Either[DecodeError, _]`, never throws.
  */
extension (frame: Frame) {

  def as[A](using ValueCodec[A]): Either[DecodeError, A] = Decode.value[A].apply(frame)

  def asArrayOf[A](using ValueCodec[A]): Either[DecodeError, Vector[A]] = Decode.vector(Decode.value[A]).apply(frame)

  // undecoded elements, for an array whose elements are heterogeneous
  def asArray: Either[DecodeError, Vector[Frame]] =
    frame match {
      case Frame.Array(elements) => Right(elements)
      case Frame.Set(elements)   => Right(elements)
      case Frame.Push(elements)  => Right(elements)
      case other                 => Left(DecodeError("array", Frame.describe(other)))
    }

  def asLong: Either[DecodeError, Long] = Decode.long(frame)

  // a string however the server framed it: simple, bulk, or verbatim
  def asString: Either[DecodeError, String] = Decode.text(frame)
}
