package sage.commands

import sage.codec.{KeyCodec, ValueCodec}

private[sage] object HyperLogLog {

  // elements is variadic, not (first, rest*): PFADD with no elements legally just creates the key
  def pfAdd[K, V](key: K, elements: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("PFADD", Command.FirstKey, keyCodec.encode(key) +: elements.toVector.map(valueCodec.encode), Decode.flag)

  // cacheable despite PFCOUNT's documented register-cache write: that write preserves the estimate, so it fires no invalidation
  def pfCount[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command.read("PFCOUNT", keys.indices.toVector, keys, Decode.long)
  }

  def pfMerge[K](destination: K, sources: K*)(using keyCodec: KeyCodec[K]): Command[Unit] = {
    val keys = (destination +: sources.toVector).map(keyCodec.encode)
    Command("PFMERGE", keys.indices.toVector, keys, Decode.ok)
  }
}
