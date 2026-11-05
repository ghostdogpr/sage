package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.Doubles
import sage.protocol.Frame
import sage.protocol.Frames.wire

/**
  * Checks that a command decodes the RESP2 and RESP3 forms of the same server reply to the same result. `resp3` is raw wire text as Redis or
  * Valkey sent it after `HELLO 3`. The RESP2 form is derived from it the way the server writes a reply after `HELLO 2`, unless a call passes
  * the RESP2 wire text for a command whose RESP2 reply has a different shape.
  */
trait BothProtocols { this: munit.FunSuite =>

  protected def decodesAlike[A](command: Command[A], resp3: String, expected: A)(using munit.Location): Unit =
    decodesBoth(command, resp2Of(wire(resp3)), wire(resp3), expected)

  protected def decodesAlike[A](command: Command[A], resp2: String, resp3: String, expected: A)(using munit.Location): Unit =
    decodesBoth(command, wire(resp2), wire(resp3), expected)

  private def decodesBoth[A](command: Command[A], resp2: Frame, resp3: Frame, expected: A)(using munit.Location): Unit = {
    assertEquals(Reply.decode(command, resp2).toEither, Right(expected), "RESP2 reply")
    assertEquals(Reply.decode(command, resp3).toEither, Right(expected), "RESP3 reply")
  }

  // RESP2 sends maps as flat key/value arrays, sets as arrays and doubles as bulk strings
  private def resp2Of(frame: Frame): Frame =
    frame match {
      case Frame.Map(entries)    => Frame.Array(entries.flatMap((key, value) => Vector(resp2Of(key), resp2Of(value))))
      case Frame.Set(elements)   => Frame.Array(elements.map(resp2Of))
      case Frame.Array(elements) => Frame.Array(elements.map(resp2Of))
      case Frame.Double(value)   => Frame.BulkString(Bytes.utf8(Doubles.format(value)))
      case other                 => other
    }

  // the shape a DecodeError names when `frame` fails to decode
  protected def expectedShape(command: Command[?], frame: Frame): Option[String] =
    Reply.decode(command, frame).failed.toOption.collect { case error: DecodeError => error.expected }
}
