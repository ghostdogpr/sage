package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * A JSONPath expression (`$.a.b`, `$..field`) addressing zero or more locations in a JSON document. Always emitted on the wire, defaulting
  * to [[JsonPath.root]] (`$`); sage neither validates nor normalizes it, so a malformed expression surfaces as a server error, not a throw.
  * Only the JSONPath (`$`) dialect is supported: every location-matching command replies one result per match, so its reply is a `Vector`.
  */
opaque type JsonPath = String

object JsonPath {

  val root: JsonPath = "$"

  def apply(expression: String): JsonPath = expression

  private[commands] def encode(path: JsonPath): Bytes = Bytes.utf8(path)
}

/**
  * Whether `JSON.SET` should write: `Always`, only `IfNotExists` (`NX`), or only `IfExists` (`XX`).
  */
enum JsonSetCondition {
  case Always, IfNotExists, IfExists
}

/**
  * The value type `JSON.TYPE` reports at a path. `Other` carries any type name a future server introduces.
  */
enum JsonType {
  case Object, Array, String, Number, Integer, Boolean, Null
  case Other(name: java.lang.String)
}

object JsonType {

  private[commands] def fromWireName(name: java.lang.String): JsonType =
    name match {
      case "object"  => Object
      case "array"   => Array
      case "string"  => String
      case "number"  => Number
      case "integer" => Integer
      case "boolean" => Boolean
      case "null"    => Null
      case other     => Other(other)
    }
}

private[sage] object Json {

  private val Nx = Bytes.utf8("NX")
  private val Xx = Bytes.utf8("XX")

  def jsonSet[K, V](key: K, path: JsonPath, value: V, condition: JsonSetCondition = JsonSetCondition.Always)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Boolean] =
    Command(
      "JSON.SET",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), valueCodec.encode(value)) ++ conditionArgs(condition),
      decode = {
        case Frame.SimpleString("OK") => Right(true)
        case Frame.Null               => Right(false)
        case other                    => Left(DecodeError("simple string 'OK' or null", Frame.describe(other)))
      }
    )

  def jsonGet[K, V](key: K, paths: JsonPath*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.read(
      "JSON.GET",
      Command.FirstKey,
      keyCodec.encode(key) +: paths.toVector.map(JsonPath.encode),
      Decode.optionalValue
    )

  def jsonMGet[K, V](path: JsonPath = JsonPath.root)(first: K, rest: K*)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] = {
    val keys = (first +: rest).iterator.map(keyCodec.encode).toVector
    Command.read("JSON.MGET", keys.indices.toVector, keys :+ JsonPath.encode(path), Decode.vector(Decode.optionalValue))
  }

  def jsonMSet[K, V](first: (K, JsonPath, V), rest: (K, JsonPath, V)*)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Unit] = {
    val triples = first +: rest.toVector
    Command("JSON.MSET", Vector.tabulate(triples.size)(_ * 3), triples.flatMap(tripleArgs), Decode.ok)
  }

  def jsonMerge[K, V](key: K, path: JsonPath, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] =
    Command("JSON.MERGE", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path), valueCodec.encode(value)), Decode.ok)

  def jsonDel[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("JSON.DEL", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), Decode.long)

  def jsonClear[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("JSON.CLEAR", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), Decode.long)

  def jsonType[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[JsonType]]] =
    Command.read("JSON.TYPE", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), typeReply)

  def jsonToggle[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Boolean]]] =
    Command("JSON.TOGGLE", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), pathMulti(optionalFlag))

  def jsonStrAppend[K, V](key: K, path: JsonPath, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[Long]]] =
    Command(
      "JSON.STRAPPEND",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), valueCodec.encode(value)),
      pathMulti(Decode.optionalLong)
    )

  def jsonStrLen[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command.read("JSON.STRLEN", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), pathMulti(Decode.optionalLong))

  def jsonNumIncrBy[K](key: K, path: JsonPath, increment: Double)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Double]]] =
    Command(
      "JSON.NUMINCRBY",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), Bytes.utf8(Doubles.format(increment))),
      numResult
    )

  def jsonNumMultBy[K](key: K, path: JsonPath, multiplier: Double)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Double]]] =
    Command(
      "JSON.NUMMULTBY",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), Bytes.utf8(Doubles.format(multiplier))),
      numResult
    )

  def jsonArrAppend[K, V](key: K, path: JsonPath, first: V, rest: V*)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[Long]]] =
    Command(
      "JSON.ARRAPPEND",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path)) ++ (first +: rest.toVector).map(valueCodec.encode),
      pathMulti(Decode.optionalLong)
    )

  def jsonArrIndex[K, V](key: K, path: JsonPath, value: V, start: Long = 0, stop: Long = 0)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[Long]]] =
    Command.read(
      "JSON.ARRINDEX",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), valueCodec.encode(value), Bytes.utf8(start.toString), Bytes.utf8(stop.toString)),
      pathMulti(Decode.optionalLong)
    )

  def jsonArrInsert[K, V](key: K, path: JsonPath, index: Long, first: V, rest: V*)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[Long]]] =
    Command(
      "JSON.ARRINSERT",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), Bytes.utf8(index.toString)) ++ (first +: rest.toVector).map(valueCodec.encode),
      pathMulti(Decode.optionalLong)
    )

  def jsonArrLen[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command.read("JSON.ARRLEN", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), pathMulti(Decode.optionalLong))

  def jsonArrPop[K, V](key: K, path: JsonPath = JsonPath.root, index: Long = -1)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[Option[V]]] =
    Command(
      "JSON.ARRPOP",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), Bytes.utf8(index.toString)),
      pathMulti(Decode.optionalValue)
    )

  def jsonArrTrim[K](key: K, path: JsonPath, start: Long, stop: Long)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command(
      "JSON.ARRTRIM",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path), Bytes.utf8(start.toString), Bytes.utf8(stop.toString)),
      pathMulti(Decode.optionalLong)
    )

  def jsonObjKeys[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Vector[String]]]] =
    Command.read("JSON.OBJKEYS", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), pathMulti(optionalKeys))

  def jsonObjLen[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command.read("JSON.OBJLEN", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), pathMulti(Decode.optionalLong))

  def jsonDebugMemory[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command.readUncacheable(
      "JSON.DEBUG MEMORY",
      Command.FirstKey,
      Vector(keyCodec.encode(key), JsonPath.encode(path)),
      pathMulti(Decode.optionalLong)
    )

  def jsonResp[K](key: K, path: JsonPath = JsonPath.root)(using keyCodec: KeyCodec[K]): Command[Frame] =
    Command.readUncacheable("JSON.RESP", Command.FirstKey, Vector(keyCodec.encode(key), JsonPath.encode(path)), Decode.frame)

  // path-multi commands reply an array, one slot per JSONPath match; a legacy (non-$) path replies a scalar, which fails here (JSONPath only)
  private def pathMulti[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    case array @ Frame.Array(_) => Decode.vector(element)(array)
    case other                  => Left(DecodeError("a JSONPath ($) reply array (legacy '.' paths are unsupported)", Frame.describe(other)))
  }

  private def conditionArgs(condition: JsonSetCondition): Vector[Bytes] =
    condition match {
      case JsonSetCondition.Always      => Vector.empty
      case JsonSetCondition.IfNotExists => Vector(Nx)
      case JsonSetCondition.IfExists    => Vector(Xx)
    }

  private def tripleArgs[K, V](triple: (K, JsonPath, V))(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Vector[Bytes] =
    Vector(keyCodec.encode(triple._1), JsonPath.encode(triple._2), valueCodec.encode(triple._3))

  private val optionalFlag: Frame => Either[DecodeError, Option[Boolean]] = {
    case Frame.Null       => Right(None)
    case Frame.Integer(0) => Right(Some(false))
    case Frame.Integer(1) => Right(Some(true))
    case other            => Left(DecodeError("integer 0 or 1, or null", Frame.describe(other)))
  }

  private val optionalKeys: Frame => Either[DecodeError, Option[Vector[String]]] = {
    case Frame.Null             => Right(None)
    case array @ Frame.Array(_) => Decode.vector(Decode.utf8String)(array).map(Some(_))
    case other                  => Left(DecodeError("array of keys, or null", Frame.describe(other)))
  }

  private val optionalTypeName: Frame => Either[DecodeError, Option[JsonType]] = {
    case Frame.Null            => Right(None)
    case Frame.SimpleString(v) => Right(Some(JsonType.fromWireName(v)))
    case Frame.BulkString(b)   => Right(Some(JsonType.fromWireName(b.asUtf8String)))
    case other                 => Left(DecodeError("type name string or null", Frame.describe(other)))
  }

  // JSON.TYPE: Redis wraps the whole type list in one outer array, Valkey replies it flat; unify to one type name per match
  private val typeReply: Frame => Either[DecodeError, Vector[Option[JsonType]]] = {
    case Frame.Array(Vector(Frame.Array(inner))) => Decode.each(inner)(optionalTypeName)
    case Frame.Array(elements)                   => Decode.each(elements)(optionalTypeName)
    case other                                   => Left(DecodeError("a JSONPath ($) reply array of type names (legacy '.' paths are unsupported)", Frame.describe(other)))
  }

  private val optionalNumber: Frame => Either[DecodeError, Option[Double]] = {
    case Frame.Null       => Right(None)
    case Frame.Integer(v) => Right(Some(v.toDouble))
    case Frame.Double(v)  => Right(Some(v))
    case other            => Left(DecodeError("number or null", Frame.describe(other)))
  }

  // JSON.NUMINCRBY: Redis replies a RESP3 number array, Valkey a JSON-array bulk string; unify to one new value per match
  private val numResult: Frame => Either[DecodeError, Vector[Option[Double]]] = {
    case Frame.Array(elements)   => Decode.each(elements)(optionalNumber)
    case Frame.BulkString(bytes) => parseNumberArray(bytes.asUtf8String)
    case other                   => Left(DecodeError("array of numbers or a JSON array string", Frame.describe(other)))
  }

  private def parseNumberArray(text: String): Either[DecodeError, Vector[Option[Double]]] = {
    val trimmed = text.trim
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]"))
      Left(DecodeError("a JSON number array (legacy '.' paths are unsupported)", s"'$trimmed'"))
    else {
      val inner = trimmed.substring(1, trimmed.length - 1).trim
      if (inner.isEmpty) Right(Vector.empty)
      else
        Decode.each(inner.split(',').toVector) { token =>
          val t = token.trim
          if (t == "null") Right(None)
          else Doubles.parse(t).map(Some(_)).toRight(DecodeError("number", t))
        }
    }
  }
}
