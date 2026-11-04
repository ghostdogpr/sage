package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * The geohash arithmetic of a Redis server, ported so that scores, `GEOHASH` strings, positions and distances match. A position is
  * stored in a sorted set as a 52-bit geohash score that interleaves 26 bits of latitude and 26 bits of longitude. Latitude is limited to
  * the range of the Web Mercator projection, as on a server.
  */
private[inmemory] object GeoHash {

  val LatMin: Double  = -85.05112878
  val LatMax: Double  = 85.05112878
  val LongMin: Double = -180.0
  val LongMax: Double = 180.0

  private val Step = 26

  // the Earth radius Redis uses for its haversine distances, in meters
  private val EarthRadius = 6372797.560856

  private val Base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  def valid(longitude: Double, latitude: Double): Boolean =
    longitude >= LongMin && longitude <= LongMax && latitude >= LatMin && latitude <= LatMax

  // spreads the low 32 bits of `v` to the even bit positions of a 64-bit value
  private def spread(v: Long): Long = {
    var x = v & 0xffffffffL
    x = (x | (x << 16)) & 0x0000ffff0000ffffL
    x = (x | (x << 8)) & 0x00ff00ff00ff00ffL
    x = (x | (x << 4)) & 0x0f0f0f0f0f0f0f0fL
    x = (x | (x << 2)) & 0x3333333333333333L
    (x | (x << 1)) & 0x5555555555555555L
  }

  // the inverse of spread: collects the even bit positions of `v`
  private def squash(v: Long): Long = {
    var x = v & 0x5555555555555555L
    x = (x | (x >>> 1)) & 0x3333333333333333L
    x = (x | (x >>> 2)) & 0x0f0f0f0f0f0f0f0fL
    x = (x | (x >>> 4)) & 0x00ff00ff00ff00ffL
    x = (x | (x >>> 8)) & 0x0000ffff0000ffffL
    (x | (x >>> 16)) & 0x00000000ffffffffL
  }

  private def encodeWith(longitude: Double, latitude: Double, latMin: Double, latMax: Double): Long = {
    val latOffset  = ((latitude - latMin) / (latMax - latMin) * (1L << Step)).toLong
    val longOffset = ((longitude - LongMin) / (LongMax - LongMin) * (1L << Step)).toLong
    // latitude takes the even bits and longitude the odd ones
    spread(latOffset) | (spread(longOffset) << 1)
  }

  def encode(longitude: Double, latitude: Double): Long = encodeWith(longitude, latitude, LatMin, LatMax)

  /**
    * The center of the cell that `hash` identifies, clamped to the valid range, as `(longitude, latitude)`.
    */
  def decode(hash: Long): (Double, Double) = {
    val latCell  = squash(hash)
    val longCell = squash(hash >>> 1)
    val cells    = (1L << Step).toDouble
    val latLow   = LatMin + latCell / cells * (LatMax - LatMin)
    val latHigh  = LatMin + (latCell + 1) / cells * (LatMax - LatMin)
    val longLow  = LongMin + longCell / cells * (LongMax - LongMin)
    val longHigh = LongMin + (longCell + 1) / cells * (LongMax - LongMin)
    (math.min(LongMax, math.max(LongMin, (longLow + longHigh) / 2)), math.min(LatMax, math.max(LatMin, (latLow + latHigh) / 2)))
  }

  /**
    * The 11-character base32 string of `GEOHASH`. A server encodes the decoded position again with the standard latitude range of -90 to
    * 90, so that the string agrees with other geohash implementations.
    */
  def text(hash: Long): String = {
    val (longitude, latitude) = decode(hash)
    val standard              = encodeWith(longitude, latitude, -90.0, 90.0)
    (0 until 11).map { i =>
      val index = if (i == 10) 0 else ((standard >>> (52 - (i + 1) * 5)) & 0x1f).toInt
      Base32(index)
    }.mkString
  }

  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val lat1r = math.toRadians(lat1)
    val lat2r = math.toRadians(lat2)
    val u     = math.sin((lat2r - lat1r) / 2)
    val v     = math.sin((math.toRadians(lon2) - math.toRadians(lon1)) / 2)
    2.0 * EarthRadius * math.asin(math.sqrt(u * u + math.cos(lat1r) * math.cos(lat2r) * v * v))
  }

  // the north-south distance between two latitudes
  def latDistance(lat1: Double, lat2: Double): Double = EarthRadius * math.abs(math.toRadians(lat2) - math.toRadians(lat1))
}

private[inmemory] object GeoCommands {
  import Replies.*
  import Value.Sorted

  val specs: Map[String, Spec] = Map(
    "GEOADD"         -> Spec(-5, geoAdd),
    "GEODIST"        -> Spec(-4, geoDist),
    "GEOHASH"        -> Spec(-2, call => Frame.Array(positions(call).map(_.fold(Frame.Null)(hash => bulk(GeoHash.text(hash)))))),
    "GEOPOS"         -> Spec(-2, call => Frame.Array(positions(call).map(_.fold(Frame.Null)(coordinates)))),
    "GEOSEARCH"      -> Spec(-7, call => search(call, store = false)),
    "GEOSEARCHSTORE" -> Spec(-8, call => search(call, store = true))
  )

  private def sorted(call: Call, key: Key): Sorted = call.typed[Sorted](key).getOrElse(Sorted.empty)

  private def coordinates(hash: Long): Frame = {
    val (longitude, latitude) = GeoHash.decode(hash)
    Frame.Array(Vector(Frame.Double(longitude), Frame.Double(latitude)))
  }

  // the geohash of each member after the key, or None for a missing member
  private def positions(call: Call): Vector[Option[Long]] = {
    val scores = sorted(call, call.key(0)).scores
    call.args.indices.drop(1).toVector.map(i => scores.get(call.key(i)).map(_.toLong))
  }

  private def unitFactor(call: Call, index: Int): Double =
    call.text(index).toLowerCase(java.util.Locale.ROOT) match {
      case "m"  => 1.0
      case "km" => 1000.0
      case "mi" => 1609.34
      case "ft" => 0.3048
      case _    => throw new Reject(Errors.err("unsupported unit provided. please use M, KM, FT, MI"))
    }

  // a server writes distances with four decimals
  private def distanceText(meters: Double, factor: Double): Frame = bulk(f"${meters / factor}%.4f")

  private def lonLat(call: Call, index: Int): (Double, Double) = {
    val longitude = call.double(index)
    val latitude  = call.double(index + 1)
    if (!GeoHash.valid(longitude, latitude))
      throw new Reject(Errors.err(f"invalid longitude,latitude pair $longitude%f,$latitude%f"))
    (longitude, latitude)
  }

  private def geoAdd(call: Call): Frame = {
    val key   = call.key(0)
    var flags = Set.empty[String]
    var i     = 1
    while (i < call.args.length && Set("NX", "XX", "CH").contains(call.keyword(i))) {
      flags += call.keyword(i)
      i += 1
    }
    if (flags("NX") && flags("XX")) throw new Reject(Errors.err("XX and NX options at the same time are not compatible"))
    val rest  = call.args.length - i
    if (rest == 0 || rest % 3 != 0) throw new Reject(Errors.syntax)
    val input   = Vector.tabulate(rest / 3) { n =>
      val (longitude, latitude) = lonLat(call, i + n * 3)
      call.key(i + n * 3 + 2) -> GeoHash.encode(longitude, latitude).toDouble
    }
    var set     = sorted(call, key)
    var added   = 0L
    var changed = 0L
    input.foreach { (member, score) =>
      val old = set.scores.get(member)
      if (!(flags("NX") && old.isDefined) && !(flags("XX") && old.isEmpty)) {
        if (old.isEmpty) added += 1
        if (!old.contains(score)) changed += 1
        set = set.updated(member, score)
      }
    }
    call.replace(key, set)
    int(if (flags("CH")) changed else added)
  }

  private def geoDist(call: Call): Frame = {
    val scores = sorted(call, call.key(0)).scores
    val factor = if (call.args.length == 4) unitFactor(call, 3) else if (call.args.length == 3) 1.0 else throw new Reject(Errors.syntax)
    (scores.get(call.key(1)), scores.get(call.key(2))) match {
      case (Some(a), Some(b)) =>
        val (lon1, lat1) = GeoHash.decode(a.toLong)
        val (lon2, lat2) = GeoHash.decode(b.toLong)
        distanceText(GeoHash.distance(lon1, lat1, lon2, lat2), factor)
      case _                  => Frame.Null
    }
  }

  private enum Shape {
    case Radius(meters: Double)
    case Box(widthMeters: Double, heightMeters: Double)
  }

  /**
    * `GEOSEARCH key origin shape [ASC | DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` and `GEOSEARCHSTORE destination source
    * … [STOREDIST]`. Every member is checked against the shape, which gives the same matches as a server's search of geohash cells.
    */
  private def search(call: Call, store: Boolean): Frame = {
    val source                                     = call.key(if (store) 1 else 0)
    val set                                        = sorted(call, source)
    var origin                                     = Option.empty[(Double, Double)]
    var shape                                      = Option.empty[Shape]
    var factor                                     = 1.0
    var ascending                                  = Option.empty[Boolean]
    var count                                      = Option.empty[(Long, Boolean)]
    var (withCoord, withDist, withHash, storeDist) = (false, false, false, false)
    var i                                          = if (store) 2 else 1
    while (i < call.args.length)
      call.keyword(i) match {
        case "FROMMEMBER" if origin.isEmpty && i + 1 < call.args.length =>
          val hash = set.scores.getOrElse(call.key(i + 1), throw new Reject(Errors.err("could not decode requested zset member")))
          origin = Some(GeoHash.decode(hash.toLong))
          i += 2
        case "FROMLONLAT" if origin.isEmpty && i + 2 < call.args.length =>
          origin = Some(lonLat(call, i + 1))
          i += 3
        case "BYRADIUS" if shape.isEmpty && i + 2 < call.args.length    =>
          factor = unitFactor(call, i + 2)
          shape = Some(Shape.Radius(call.double(i + 1) * factor))
          i += 3
        case "BYBOX" if shape.isEmpty && i + 3 < call.args.length       =>
          factor = unitFactor(call, i + 3)
          shape = Some(Shape.Box(call.double(i + 1) * factor, call.double(i + 2) * factor))
          i += 4
        case "ASC"                                                      => ascending = Some(true); i += 1
        case "DESC"                                                     => ascending = Some(false); i += 1
        case "COUNT" if i + 1 < call.args.length                        =>
          val n   = call.long(i + 1)
          if (n <= 0) throw new Reject(Errors.err("COUNT must be > 0"))
          val any = call.args.lift(i + 2).exists(_.asUtf8String.equalsIgnoreCase("ANY"))
          count = Some((n, any))
          i += (if (any) 3 else 2)
        case "WITHCOORD" if !store                                      => withCoord = true; i += 1
        case "WITHDIST" if !store                                       => withDist = true; i += 1
        case "WITHHASH" if !store                                       => withHash = true; i += 1
        case "STOREDIST" if store                                       => storeDist = true; i += 1
        case _                                                          => throw new Reject(Errors.syntax)
      }
    val (centerLon, centerLat)                     =
      origin.getOrElse(throw new Reject(Errors.err("exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch")))
    val area                                       = shape.getOrElse(throw new Reject(Errors.err("exactly one of BYRADIUS and BYBOX can be specified for geosearch")))
    val matches                                    = set.ordered.toVector.flatMap { (score, member) =>
      val (lon, lat) = GeoHash.decode(score.toLong)
      val inside     = area match {
        case Shape.Radius(meters)     => Option(GeoHash.distance(centerLon, centerLat, lon, lat)).filter(_ <= meters)
        // a box is checked north-south first, then east-west along the member's latitude, as on a server
        case Shape.Box(width, height) =>
          Option.when(
            GeoHash.latDistance(lat, centerLat) <= height / 2 && GeoHash.distance(lon, lat, centerLon, lat) <= width / 2
          )(GeoHash.distance(centerLon, centerLat, lon, lat))
      }
      inside.map(distance => (member, score.toLong, distance))
    }
    // COUNT without ANY sorts by distance, as a server does
    val order                                      = ascending.orElse(Option.when(count.exists(!_._2))(true))
    val ordered                                    = order.fold(matches)(asc => if (asc) matches.sortBy(_._3) else matches.sortBy(-_._3))
    val limited                                    = count.fold(ordered)((n, _) => ordered.take(math.min(n, Int.MaxValue.toLong).toInt))
    if (store) {
      val destination = call.key(0)
      call.store.delete(destination, call.now): Unit
      call.replace(destination, Sorted.of(limited.map((member, hash, distance) => member -> (if (storeDist) distance / factor else hash.toDouble))))
      int(limited.length.toLong)
    } else if (!withCoord && !withDist && !withHash) keys(limited.map(_._1))
    else
      Frame.Array(limited.map { (member, hash, distance) =>
        Frame.Array(
          Vector(bulk(member.bytes)) ++
            Option.when(withDist)(distanceText(distance, factor)) ++
            Option.when(withHash)(int(hash)) ++
            Option.when(withCoord)(coordinates(hash))
        )
      })
  }
}
