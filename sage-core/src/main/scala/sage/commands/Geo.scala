package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.{Ch, Count, Nx, Xx}
import sage.protocol.Frame

/**
  * A geographic coordinate. Longitude comes first to match the command syntax; the field names help prevent the two values being swapped.
  */
final case class GeoCoordinates(longitude: Double, latitude: Double)

/**
  * A distance unit, shared across `GEODIST`/`GEOSEARCH`/`GEOSEARCHSTORE`.
  */
enum GeoUnit {
  case Meters, Kilometers, Miles, Feet
}

/**
  * When `GEOADD` should write a member: `Always`, only `IfNotExists` (`NX`), or only `IfExists` (`XX`).
  */
enum GeoAddCondition {
  case Always, IfNotExists, IfExists
}

/**
  * The center a geo search is measured from: an existing `FromMember`, or an explicit `FromLonLat` coordinate.
  */
enum GeoOrigin[+V] {
  case FromMember(member: V)
  case FromLonLat(coordinates: GeoCoordinates) extends GeoOrigin[Nothing]
}

/**
  * The search area: a circle `ByRadius` or a rectangle `ByBox`, each in a [[GeoUnit]].
  */
enum GeoShape {
  case ByRadius(radius: Double, unit: GeoUnit)
  case ByBox(width: Double, height: Double, unit: GeoUnit)
}

/**
  * Result ordering by distance from the origin: `Asc` nearest-first, `Desc` farthest-first.
  */
enum GeoSort {
  case Asc, Desc
}

/**
  * A search result limit. `any` returns the first `count` matches found rather than the `count` nearest, which is faster but unordered.
  */
final case class GeoCount(count: Long, any: Boolean = false)

/**
  * One geo search match. Each projection is `Some` only when its corresponding `WITH` flag was requested.
  */
final case class GeoSearchResult[V](member: V, distance: Option[Double], hash: Option[Long], coordinates: Option[GeoCoordinates])

private[sage] object Geo {

  private val FromMember     = Bytes.utf8("FROMMEMBER")
  private val FromLonLat     = Bytes.utf8("FROMLONLAT")
  private val ByRadius       = Bytes.utf8("BYRADIUS")
  private val ByBox          = Bytes.utf8("BYBOX")
  private val sortWord       = Args.keywords(GeoSort.values)
  private val AnyWord        = Bytes.utf8("ANY")
  private val WithCoord      = Bytes.utf8("WITHCOORD")
  private val WithDist       = Bytes.utf8("WITHDIST")
  private val WithHash       = Bytes.utf8("WITHHASH")
  private val StoreDist      = Bytes.utf8("STOREDIST")
  private val UnitMeters     = Bytes.utf8("m")
  private val UnitKilometers = Bytes.utf8("km")
  private val UnitMiles      = Bytes.utf8("mi")
  private val UnitFeet       = Bytes.utf8("ft")

  def geoAdd[K, V](key: K, condition: GeoAddCondition = GeoAddCondition.Always, changed: Boolean = false)(
    first: (V, GeoCoordinates),
    rest: (V, GeoCoordinates)*
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "GEOADD",
      Command.FirstKey,
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ Args.flag(changed, Ch) ++ memberCoordArgs(first +: rest.toVector),
      Decode.long
    )

  def geoDist[K, V](key: K, member1: V, member2: V, unit: GeoUnit = GeoUnit.Meters)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[Double]] =
    Command.read(
      "GEODIST",
      Command.FirstKey,
      Vector(keyCodec.encode(key), valueCodec.encode(member1), valueCodec.encode(member2), unitArg(unit)),
      Decode.optionalDouble
    )

  def geoHash[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[String]]] =
    Command.read("GEOHASH", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.vector(Decode.optionalUtf8String))

  def geoPos[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[GeoCoordinates]]] =
    Command.read("GEOPOS", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.vector(optionalCoordinates))

  def geoSearch[K, V](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.read(
      "GEOSEARCH",
      Command.FirstKey,
      keyCodec.encode(key) +: searchArgs(origin, shape, sort, count),
      Decode.vector(Decode.value[V])
    )

  def geoSearchWith[K, V](
    key: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    withCoord: Boolean = false,
    withDist: Boolean = false,
    withHash: Boolean = false,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[GeoSearchResult[V]]] =
    Command.read(
      "GEOSEARCH",
      Command.FirstKey,
      keyCodec.encode(key) +: (searchArgs(origin, shape, sort, count) ++ withArgs(withCoord, withDist, withHash)),
      searchReply[V](withCoord, withDist, withHash)
    )

  def geoSearchStore[K, V](
    destination: K,
    source: K,
    origin: GeoOrigin[V],
    shape: GeoShape,
    sort: Option[GeoSort] = None,
    count: Option[GeoCount] = None,
    storeDist: Boolean = false
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "GEOSEARCHSTORE",
      Vector(0, 1),
      (Vector(keyCodec.encode(destination), keyCodec.encode(source)) ++ searchArgs(origin, shape, sort, count)) ++ Args.flag(storeDist, StoreDist),
      Decode.long
    )

  private def conditionArgs(condition: GeoAddCondition): Vector[Bytes] =
    condition match {
      case GeoAddCondition.Always      => Vector.empty
      case GeoAddCondition.IfNotExists => Vector(Nx)
      case GeoAddCondition.IfExists    => Vector(Xx)
    }

  private def memberCoordArgs[V](pairs: Vector[(V, GeoCoordinates)])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (member, coords) => Vector(Args.double(coords.longitude), Args.double(coords.latitude), valueCodec.encode(member)) }

  private def originArgs[V](origin: GeoOrigin[V])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    origin match {
      case GeoOrigin.FromMember(member) => Vector(FromMember, valueCodec.encode(member))
      case GeoOrigin.FromLonLat(coords) => Vector(FromLonLat, Args.double(coords.longitude), Args.double(coords.latitude))
    }

  private def shapeArgs(shape: GeoShape): Vector[Bytes] =
    shape match {
      case GeoShape.ByRadius(radius, unit)     => Vector(ByRadius, Args.double(radius), unitArg(unit))
      case GeoShape.ByBox(width, height, unit) => Vector(ByBox, Args.double(width), Args.double(height), unitArg(unit))
    }

  private def searchArgs[V: ValueCodec](origin: GeoOrigin[V], shape: GeoShape, sort: Option[GeoSort], count: Option[GeoCount]): Vector[Bytes] =
    originArgs(origin) ++ shapeArgs(shape) ++ sort.toVector.map(sortWord) ++
      count.toVector.flatMap(c => Vector(Count, Args.long(c.count)) ++ Args.flag(c.any, AnyWord))

  private def withArgs(withCoord: Boolean, withDist: Boolean, withHash: Boolean): Vector[Bytes] =
    Args.flag(withCoord, WithCoord) ++
      Args.flag(withDist, WithDist) ++
      Args.flag(withHash, WithHash)

  private def unitArg(unit: GeoUnit): Bytes =
    unit match {
      case GeoUnit.Meters     => UnitMeters
      case GeoUnit.Kilometers => UnitKilometers
      case GeoUnit.Miles      => UnitMiles
      case GeoUnit.Feet       => UnitFeet
    }

  private val coordinates: Frame => Either[DecodeError, GeoCoordinates] =
    Decode.array2(Decode.double, Decode.double, "longitude/latitude pair")(GeoCoordinates(_, _))

  private val optionalCoordinates: Frame => Either[DecodeError, Option[GeoCoordinates]] = Decode.nullable(coordinates)

  // Without any WITH flag GEOSEARCH replies a flat array of member bulk strings; any flag turns each row into [member, dist?, hash?, coord?]
  // in that fixed field order regardless of the order the flags were requested
  private def searchReply[V](withCoord: Boolean, withDist: Boolean, withHash: Boolean)(
    using valueCodec: ValueCodec[V]
  ): Frame => Either[DecodeError, Vector[GeoSearchResult[V]]] = {
    val enriched = withCoord || withDist || withHash
    if (!enriched) Decode.vector(frame => Decode.value[V](frame).map(member => GeoSearchResult(member, None, None, None)))
    else {
      val width    = 1 + (if (withDist) 1 else 0) + (if (withHash) 1 else 0) + (if (withCoord) 1 else 0)
      val distIdx  = 1
      val hashIdx  = distIdx + (if (withDist) 1 else 0)
      val coordIdx = hashIdx + (if (withHash) 1 else 0)
      Decode.vector {
        case Frame.Array(fields) if fields.length == width =>
          for {
            member   <- Decode.value[V](fields(0))
            distance <- if (withDist) Decode.double(fields(distIdx)).map(Some(_)) else Right(None)
            hash     <- if (withHash) Decode.long(fields(hashIdx)).map(Some(_)) else Right(None)
            coords   <- if (withCoord) coordinates(fields(coordIdx)).map(Some(_)) else Right(None)
          } yield GeoSearchResult(member, distance, hash, coords)
        case other                                         => Left(DecodeError(s"geo result of $width elements", Frame.describe(other)))
      }
    }
  }
}
