package sage.commands

import sage.Bytes
import sage.codec.KeyCodec

private[commands] object KeyArgs {

  // the `numkeys key…` block: encoded keys prefixed by their count, with 1-based key positions
  def numKeyed[K](keys: Vector[K])(using keyCodec: KeyCodec[K]): (Vector[Int], Vector[Bytes]) = {
    val encoded = keys.map(keyCodec.encode)
    (Vector.tabulate(encoded.size)(_ + 1), Bytes.utf8(encoded.size.toString) +: encoded)
  }
}
