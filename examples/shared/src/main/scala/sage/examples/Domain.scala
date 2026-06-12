package sage.examples

import sage.*

/**
  * A tiny domain type shared by every backend's tour. It rides on a hand-written [[ValueCodec]], so the tours can `set`/`get` a `User` the
  * same way they do a `String` — the whole point of the codec typeclass. Backend-independent (only `import sage.*`), so it also compiles in
  * the build's compile-only `future` anchor cell.
  */
final case class User(name: String, age: Int)

object User {

  // Encodes as "name|age" and decodes strictly: anything that is not that shape fails with a DecodeError rather than being coerced, matching
  // the contract of the built-in codecs. `emap` is the partial-decode combinator; `imap` is its total counterpart. Age is the trailing
  // segment, so the name may itself contain '|'.
  given ValueCodec[User] =
    ValueCodec[String].emap { raw =>
      raw.lastIndexOf('|') match {
        case -1 => Left(SageException.DecodeError("User(name|age)", raw))
        case i  => raw.drop(i + 1).toIntOption.map(User(raw.take(i), _)).toRight(SageException.DecodeError("User(name|age)", raw))
      }
    }(user => s"${user.name}|${user.age}")
}
