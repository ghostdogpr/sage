package sage.commands

import sage.codec.{KeyCodec, ValueCodec}

private[sage] object HyperLogLog {

  // allow an empty elements parameter because PFADD key is valid and creates the key
  def pfAdd[K, V](key: K, elements: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("PFADD", Command.FirstKey, keyCodec.encode(key) +: elements.toVector.map(valueCodec.encode), Decode.flag)

  // PFCOUNT is cacheable. Its documented internal register-cache write does not change the estimate or send an invalidation.
  def pfCount[K](first: K, rest: K*)(using KeyCodec[K]): Command[Long] =
    KeyArgs.allKeys("PFCOUNT", first +: rest.toVector, Decode.long, readOnly = true)

  def pfMerge[K](destination: K, sources: K*)(using KeyCodec[K]): Command[Unit] =
    KeyArgs.allKeys("PFMERGE", destination +: sources.toVector, Decode.ok)
}
