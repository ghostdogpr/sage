package sage.examples

import sage.*

/**
  * A small domain type shared by the backend examples. Its [[ValueCodec]] allows Sage to store and retrieve a `User` directly.
  */
final case class User(name: String, age: Int)

object User {

  // Encode as "name|age". Invalid input produces a DecodeError, as it does for the built-in codecs. Splitting at the final separator allows
  // the name itself to contain '|'. Use emap because decoding can fail; imap is available when both conversions are total.
  given ValueCodec[User] =
    ValueCodec[String].emap { raw =>
      raw.lastIndexOf('|') match {
        case -1 => Left(SageException.DecodeError("User(name|age)", raw))
        case i  => raw.drop(i + 1).toIntOption.map(User(raw.take(i), _)).toRight(SageException.DecodeError("User(name|age)", raw))
      }
    }(user => s"${user.name}|${user.age}")
}
