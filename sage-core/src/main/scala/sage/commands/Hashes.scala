package sage.commands

import sage.Bytes
import sage.codec.{KeyCodec, ValueCodec}

/**
  * A hash field is an identifier within the hash, never cluster-routed, so it takes a [[KeyCodec]] (not a [[ValueCodec]]): the same
  * representation-stability guarantee a key needs.
  */
object Hashes {

  def hSet[K, F, V](key: K, first: (F, V), rest: (F, V)*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Long] =
    Command("HSET", Command.FirstKey, keyCodec.encode(key) +: fieldValueArgs(first +: rest.toVector), Decode.long)

  def hSetNx[K, F, V](key: K, field: F, value: V)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Boolean] =
    Command("HSETNX", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), valueCodec.encode(value)), Decode.flag)

  def hGet[K, F, V](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command("HGET", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.optionalValue)

  def hmGet[K, F, V](key: K, first: F, rest: F*)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command("HMGET", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(fieldCodec.encode), Decode.vector(Decode.optionalValue))

  def hDel[K, F](key: K, first: F, rest: F*)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HDEL", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(fieldCodec.encode), Decode.long)

  def hExists[K, F](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Boolean] =
    Command("HEXISTS", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.flag)

  def hLen[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("HLEN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def hStrLen[K, F](key: K, field: F)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HSTRLEN", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field)), Decode.long)

  def hKeys[K, F](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[F]] =
    Command("HKEYS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.vector(Decode.key[F]))

  def hVals[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command("HVALS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.vector(Decode.value[V]))

  def hGetAll[K, F, V](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Map[F, V]] =
    Command("HGETALL", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.map[F, V])

  def hIncrBy[K, F](key: K, field: F, increment: Long)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Long] =
    Command("HINCRBY", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), Bytes.utf8(increment.toString)), Decode.long)

  def hIncrByFloat[K, F](key: K, field: F, increment: Double)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Double] =
    Command("HINCRBYFLOAT", Command.FirstKey, Vector(keyCodec.encode(key), fieldCodec.encode(field), Bytes.utf8(increment.toString)), Decode.double)

  def hRandField[K, F](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Option[F]] =
    Command("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalKey[F])

  def hRandField[K, F](key: K, count: Long)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F]): Command[Vector[F]] =
    Command("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vector(Decode.key[F]))

  def hRandFieldWithValues[K, F, V](
    key: K,
    count: Long
  )(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[Vector[(F, V)]] =
    Command("HRANDFIELD", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString), WithValues), Decode.nestedPairs[F, V])

  def hScan[K, F, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[(F, V)]] =
    Command(
      "HSCAN",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ scanOptions(pattern, count),
      Decode.scanPage(Decode.flatPairs[F, V])
    )

  def hScanNoValues[K, F](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F]
  ): Command[ScanPage[F]] =
    Command(
      "HSCAN",
      Command.FirstKey,
      (Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ scanOptions(pattern, count)) :+ NoValues,
      Decode.scanPage(Decode.vector(Decode.key[F]))
    )

  private def fieldValueArgs[F, V](pairs: Vector[(F, V)])(using fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (field, value) => Vector(fieldCodec.encode(field), valueCodec.encode(value)) }

  private def scanOptions(pattern: Option[String], count: Option[Long]): Vector[Bytes] =
    pattern.toVector.flatMap(p => Vector(Match, Bytes.utf8(p))) ++ count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString)))

  private val Match      = Bytes.utf8("MATCH")
  private val Count      = Bytes.utf8("COUNT")
  private val WithValues = Bytes.utf8("WITHVALUES")
  private val NoValues   = Bytes.utf8("NOVALUES")
}
