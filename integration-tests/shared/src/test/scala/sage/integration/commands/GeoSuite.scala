package sage.integration.commands

import kyo.compat.*

import sage.commands.{GeoCoordinates, GeoCount, GeoOrigin, GeoShape, GeoSort, GeoUnit}
import sage.integration.{Images, ServerSuite}

abstract class GeoSuite(image: String) extends ServerSuite(image) {

  private val palermo = GeoCoordinates(13.361389, 38.115556)
  private val catania = GeoCoordinates(15.087269, 37.502669)

  test("GEOADD GEOPOS GEODIST and GEOHASH store and read positions") {
    withClient { client =>
      for {
        added <- client.geoAdd("Sicily")(("Palermo", palermo), ("Catania", catania))
        dup   <- client.geoAdd("Sicily", changed = true)(("Palermo", palermo))
        pos   <- client.geoPos("Sicily", "Palermo", "NonExisting")
        dist  <- client.geoDist("Sicily", "Palermo", "Catania", GeoUnit.Kilometers)
        hash  <- client.geoHash("Sicily", "Palermo", "NonExisting")
      } yield {
        assertEquals(added, 2L)
        assertEquals(dup, 0L)
        assertEquals(pos.size, 2)
        assert(pos(0).exists(c => Math.abs(c.longitude - palermo.longitude) < 0.001 && Math.abs(c.latitude - palermo.latitude) < 0.001))
        assertEquals(pos(1), None)
        assert(dist.exists(d => d > 166.0 && d < 167.0))
        assert(hash(0).exists(_.startsWith("sqc8b49rny")))
        assertEquals(hash(1), None)
      }
    }
  }

  test("GEOSEARCH returns members within an area, ordered and limited") {
    withClient { client =>
      for {
        _       <- client.geoAdd("Sicily2")(("Palermo", palermo), ("Catania", catania))
        members <- client.geoSearch[String, String](
                     "Sicily2",
                     GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)),
                     GeoShape.ByRadius(200.0, GeoUnit.Kilometers),
                     sort = Some(GeoSort.Asc)
                   )
        nearest <- client.geoSearch(
                     "Sicily2",
                     GeoOrigin.FromMember("Palermo"),
                     GeoShape.ByBox(400.0, 400.0, GeoUnit.Kilometers),
                     count = Some(GeoCount(1))
                   )
      } yield {
        assertEquals(members, Vector("Catania", "Palermo"))
        assertEquals(nearest, Vector("Palermo"))
      }
    }
  }

  test("GEOSEARCH with projections returns coordinates, distance and hash") {
    withClient { client =>
      for {
        _    <- client.geoAdd("Sicily3")(("Palermo", palermo), ("Catania", catania))
        hits <- client.geoSearchWith(
                  "Sicily3",
                  GeoOrigin.FromMember("Palermo"),
                  GeoShape.ByRadius(200.0, GeoUnit.Kilometers),
                  withCoord = true,
                  withDist = true,
                  withHash = true,
                  sort = Some(GeoSort.Asc)
                )
      } yield {
        assertEquals(hits.map(_.member), Vector("Palermo", "Catania"))
        assert(hits.forall(h => h.distance.isDefined && h.hash.isDefined && h.coordinates.isDefined))
        assert(hits.head.distance.exists(_ < 1.0))
      }
    }
  }

  test("GEOSEARCHSTORE writes matches into a destination key") {
    withClient { client =>
      for {
        _       <- client.geoAdd("Sicily4")(("Palermo", palermo), ("Catania", catania))
        stored  <- client.geoSearchStore[String, String](
                     "Sicily4Store",
                     "Sicily4",
                     GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)),
                     GeoShape.ByRadius(200.0, GeoUnit.Kilometers)
                   )
        members <- client.geoSearch[String, String](
                     "Sicily4Store",
                     GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.0)),
                     GeoShape.ByRadius(200.0, GeoUnit.Kilometers)
                   )
      } yield {
        assertEquals(stored, 2L)
        assertEquals(members.toSet, Set("Palermo", "Catania"))
      }
    }
  }
}

class RedisGeoSuite extends GeoSuite(Images.redis)

class ValkeyGeoSuite extends GeoSuite(Images.valkey)
