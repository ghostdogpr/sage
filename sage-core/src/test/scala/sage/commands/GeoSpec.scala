package sage.commands

class GeoSpec extends munit.FunSuite with BothProtocols {

  test("GEOPOS decodes RESP2 bulk-string coordinates like RESP3 doubles, and a missing member as None") {
    decodesAlike(
      Geo.geoPos("g", "Palermo", "nobody"),
      "*2\r\n*2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n*-1\r\n",
      "*2\r\n*2\r\n,13.361389338970184\r\n,38.1155563954963\r\n_\r\n",
      Vector(Some(GeoCoordinates(13.361389338970184, 38.1155563954963)), None)
    )
  }

  test("GEOSEARCH with every WITH option decodes RESP2 coordinates like RESP3") {
    decodesAlike(
      Geo.geoSearchWith[String, String](
        "g",
        GeoOrigin.FromLonLat(GeoCoordinates(15, 37)),
        GeoShape.ByRadius(200, GeoUnit.Kilometers),
        withCoord = true,
        withDist = true,
        withHash = true
      ),
      "*1\r\n*4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n",
      "*1\r\n*4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n*2\r\n,15.087267458438873\r\n,37.50266842333162\r\n",
      Vector(GeoSearchResult("Catania", Some(56.4413), Some(3479447370796909L), Some(GeoCoordinates(15.087267458438873, 37.50266842333162))))
    )
  }

  test("GEODIST decodes the distance and a missing member under both protocols") {
    decodesAlike(Geo.geoDist("g", "Palermo", "Catania", GeoUnit.Kilometers), "$8\r\n166.2742\r\n", "$8\r\n166.2742\r\n", Some(166.2742))
    decodesAlike(Geo.geoDist("g", "Palermo", "nobody"), "$-1\r\n", "_\r\n", None)
  }
}
