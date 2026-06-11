package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.protocol.Frame

// longitude first, matching the wire order; named so the two same-typed coordinates cannot be silently swapped
final case class GeoCoordinates(longitude: Double, latitude: Double)

// shared across GEODIST/GEOSEARCH/GEOSEARCHSTORE: one distance-unit primitive, not a per-command option group
enum GeoUnit {
  case Meters, Kilometers, Miles, Feet
}

enum GeoAddCondition {
  case Always, IfNotExists, IfExists
}

enum GeoOrigin[+V] {
  case FromMember(member: V)
  case FromLonLat(coordinates: GeoCoordinates) extends GeoOrigin[Nothing]
}

enum GeoShape {
  case ByRadius(radius: Double, unit: GeoUnit)
  case ByBox(width: Double, height: Double, unit: GeoUnit)
}

// Asc is nearest-first, Desc farthest-first
enum GeoSort {
  case Asc, Desc
}

// ANY cannot be requested without a count, so the two travel together
final case class GeoCount(count: Long, any: Boolean = false)

// each projection is Some only when its WITH flag was requested
final case class GeoSearchResult[V](member: V, distance: Option[Double], hash: Option[Long], coordinates: Option[GeoCoordinates])

private[sage] object Geo {

  private val Nx             = Bytes.utf8("NX")
  private val Xx             = Bytes.utf8("XX")
  private val Ch             = Bytes.utf8("CH")
  private val FromMember     = Bytes.utf8("FROMMEMBER")
  private val FromLonLat     = Bytes.utf8("FROMLONLAT")
  private val ByRadius       = Bytes.utf8("BYRADIUS")
  private val ByBox          = Bytes.utf8("BYBOX")
  private val Asc            = Bytes.utf8("ASC")
  private val Desc           = Bytes.utf8("DESC")
  private val CountWord      = Bytes.utf8("COUNT")
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
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ (if (changed) Vector(Ch) else Vector.empty) ++ memberCoordArgs(first +: rest.toVector),
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
    Command.read(
      "GEOHASH",
      Command.FirstKey,
      keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode),
      Decode.vector(Decode.optionalUtf8String)
    )

  def geoPos[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[GeoCoordinates]]] =
    Command.read(
      "GEOPOS",
      Command.FirstKey,
      keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode),
      Decode.vector(optionalCoordinates)
    )

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
      keyCodec.encode(key) +: (originArgs(origin) ++ shapeArgs(shape) ++ sortArgs(sort) ++ countArgs(count)),
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
      keyCodec
        .encode(key) +: (originArgs(origin) ++ shapeArgs(shape) ++ sortArgs(sort) ++ countArgs(count) ++ withArgs(withCoord, withDist, withHash)),
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
      (Vector(keyCodec.encode(destination), keyCodec.encode(source)) ++ originArgs(origin) ++ shapeArgs(shape) ++ sortArgs(sort) ++ countArgs(
        count
      )) ++
        (if (storeDist) Vector(StoreDist) else Vector.empty),
      Decode.long
    )

  private def conditionArgs(condition: GeoAddCondition): Vector[Bytes] =
    condition match {
      case GeoAddCondition.Always      => Vector.empty
      case GeoAddCondition.IfNotExists => Vector(Nx)
      case GeoAddCondition.IfExists    => Vector(Xx)
    }

  private def memberCoordArgs[V](pairs: Vector[(V, GeoCoordinates)])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (member, coords) => Vector(coordArg(coords.longitude), coordArg(coords.latitude), valueCodec.encode(member)) }

  private def originArgs[V](origin: GeoOrigin[V])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    origin match {
      case GeoOrigin.FromMember(member) => Vector(FromMember, valueCodec.encode(member))
      case GeoOrigin.FromLonLat(coords) => Vector(FromLonLat, coordArg(coords.longitude), coordArg(coords.latitude))
    }

  private def shapeArgs(shape: GeoShape): Vector[Bytes] =
    shape match {
      case GeoShape.ByRadius(radius, unit)     => Vector(ByRadius, Bytes.utf8(Doubles.format(radius)), unitArg(unit))
      case GeoShape.ByBox(width, height, unit) => Vector(ByBox, Bytes.utf8(Doubles.format(width)), Bytes.utf8(Doubles.format(height)), unitArg(unit))
    }

  private def sortArgs(sort: Option[GeoSort]): Vector[Bytes] =
    sort.toVector.map {
      case GeoSort.Asc  => Asc
      case GeoSort.Desc => Desc
    }

  private def countArgs(count: Option[GeoCount]): Vector[Bytes] =
    count.toVector.flatMap(c => Vector(CountWord, Bytes.utf8(c.count.toString)) ++ (if (c.any) Vector(AnyWord) else Vector.empty))

  private def withArgs(withCoord: Boolean, withDist: Boolean, withHash: Boolean): Vector[Bytes] =
    (if (withCoord) Vector(WithCoord) else Vector.empty) ++
      (if (withDist) Vector(WithDist) else Vector.empty) ++
      (if (withHash) Vector(WithHash) else Vector.empty)

  private def unitArg(unit: GeoUnit): Bytes =
    unit match {
      case GeoUnit.Meters     => UnitMeters
      case GeoUnit.Kilometers => UnitKilometers
      case GeoUnit.Miles      => UnitMiles
      case GeoUnit.Feet       => UnitFeet
    }

  private def coordArg(value: Double): Bytes = Bytes.utf8(Doubles.format(value))

  private val coordinates: Frame => Either[DecodeError, GeoCoordinates] = {
    case Frame.Array(Vector(lon, lat)) =>
      for {
        x <- Decode.lenientDouble(lon)
        y <- Decode.lenientDouble(lat)
      } yield GeoCoordinates(x, y)
    case other                         => Left(DecodeError("longitude/latitude pair", Frame.describe(other)))
  }

  private val optionalCoordinates: Frame => Either[DecodeError, Option[GeoCoordinates]] = {
    case Frame.Null => Right(None)
    case other      => coordinates(other).map(Some(_))
  }

  // without any WITH flag GEOSEARCH replies a flat array of member bulk strings; any flag turns each row into [member, dist?, hash?, coord?]
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
            distance <- if (withDist) Decode.lenientDouble(fields(distIdx)).map(Some(_)) else Right(None)
            hash     <- if (withHash) Decode.long(fields(hashIdx)).map(Some(_)) else Right(None)
            coords   <- if (withCoord) coordinates(fields(coordIdx)).map(Some(_)) else Right(None)
          } yield GeoSearchResult(member, distance, hash, coords)
        case other                                         => Left(DecodeError(s"geo result of $width elements", Frame.describe(other)))
      }
    }
  }
}
