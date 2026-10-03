package sage.integration.commands

import kyo.compat.*

import sage.commands.{GeoCoordinates, GeoCount, GeoOrigin, GeoShape, GeoSort, GeoUnit}
import sage.integration.BothServersSuite

class GeoSuite extends BothServersSuite {

  private val palermo = GeoCoordinates(13.361389, 38.115556)
  private val catania = GeoCoordinates(15.087269, 37.502669)

  clientTest("GEOADD GEOPOS GEODIST and GEOHASH store and read positions") { client =>
    client.geoAdd("Sicily")(("Palermo", palermo), ("Catania", catania)).is(2L) >>
      client.geoAdd("Sicily", changed = true)(("Palermo", palermo)).is(0L) >>
      client
        .geoPos("Sicily", "Palermo", "NonExisting")
        .map(_.map(_.map(c => (c.longitude - palermo.longitude).abs < 0.001 && (c.latitude - palermo.latitude).abs < 0.001)))
        .is(Vector(Some(true), None)) >>
      client.geoDist("Sicily", "Palermo", "Catania", GeoUnit.Kilometers).satisfies(_.exists(d => d > 166.0 && d < 167.0)) >>
      client.geoHash("Sicily", "Palermo", "NonExisting").map(_.map(_.map(_.startsWith("sqc8b49rny")))).is(Vector(Some(true), None))
  }

  clientTest("GEOSEARCH returns members within an area, ordered and limited") { client =>
    client.geoAdd("Sicily2")(("Palermo", palermo), ("Catania", catania)) >>
      client
        .geoSearch[String](
          "Sicily2",
          GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)),
          GeoShape.ByRadius(200.0, GeoUnit.Kilometers),
          sort = Some(GeoSort.Asc)
        )
        .is(Vector("Catania", "Palermo")) >>
      client
        .geoSearch("Sicily2", GeoOrigin.FromMember("Palermo"), GeoShape.ByBox(400.0, 400.0, GeoUnit.Kilometers), count = Some(GeoCount(1)))
        .is(Vector("Palermo"))
  }

  clientTest("GEOSEARCH with projections returns coordinates, distance and hash") { client =>
    client.geoAdd("Sicily3")(("Palermo", palermo), ("Catania", catania)) >>
      client
        .geoSearchWith(
          "Sicily3",
          GeoOrigin.FromMember("Palermo"),
          GeoShape.ByRadius(200.0, GeoUnit.Kilometers),
          withCoord = true,
          withDist = true,
          withHash = true,
          sort = Some(GeoSort.Asc)
        )
        .map(_.map(h => (h.member, h.distance.map(_.round), h.hash.isDefined, h.coordinates.isDefined)))
        .is(Vector(("Palermo", Some(0L), true, true), ("Catania", Some(166L), true, true)))
  }

  clientTest("GEOSEARCHSTORE writes matches into a destination key") { client =>
    val area = GeoShape.ByRadius(200.0, GeoUnit.Kilometers)
    client.geoAdd("Sicily4")(("Palermo", palermo), ("Catania", catania)) >>
      client.geoSearchStore[String]("Sicily4Store", "Sicily4", GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)), area).is(2L) >>
      client.geoSearch[String]("Sicily4Store", GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)), area).map(_.toSet).is(Set("Palermo", "Catania"))
  }
}
