package sage.commands

import sage.codec.{KeyCodec, ValueCodec}

private[sage] object HyperLogLog {

  // allow an empty elements parameter because PFADD key is valid and creates the key
  def pfAdd[K, V](key: K, elements: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("PFADD", Command.FirstKey, keyCodec.encode(key) +: elements.toVector.map(valueCodec.encode), Decode.flag)

  // PFCOUNT is cacheable. Its documented internal register-cache write does not change the estimate or send an invalidation.
  def pfCount[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val keys = (first +: rest).iterator.map(keyCodec.encode).toVector
    Command.read("PFCOUNT", keys.indices.toVector, keys, Decode.long)
  }

  def pfMerge[K](destination: K, sources: K*)(using keyCodec: KeyCodec[K]): Command[Unit] = {
    val keys = (destination +: sources).iterator.map(keyCodec.encode).toVector
    Command("PFMERGE", keys.indices.toVector, keys, Decode.ok)
  }
}
