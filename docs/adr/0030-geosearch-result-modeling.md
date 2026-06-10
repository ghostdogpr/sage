# GEOSEARCH result modeling

`GEOSEARCH` has three independent projection flags — `WITHCOORD`, `WITHDIST`, `WITHHASH` — each of which adds a field to every result element. ADR-0011's rule that "an option that changes the reply type becomes a separate builder" (the `setGet` / `zRangeWithScores` split) does not scale here: three combinable flags would demand up to eight builders. Sage instead bends the rule to a binary split at the meaningful boundary — bare members vs enriched records:

- `geoSearch(key, origin, shape, sort, count): Command[Vector[V]]` — members only, the clean common path, exactly like `zRange`.
- `geoSearchWith(key, origin, shape, withCoord, withDist, withHash, sort, count): Command[Vector[GeoSearchResult[V]]]`, where `GeoSearchResult[V](member, distance: Option[Double], hash: Option[Long], coordinates: Option[GeoCoordinates])` carries one `Option` field per projection — `Some` iff the flag was requested.

`GEOSEARCHSTORE` reuses the same query types and keeps its own builder returning `Command[Long]` (with a trailing `storeDist: Boolean`), the way `ZRANGESTORE` reuses `ZRange`.

## Origin and shape are sealed query primitives

The search origin and search area are two independent mutually-exclusive choices, each modeled as a sealed enum carrying exactly its legal arguments, in the `ZRange` style:

- `GeoOrigin[+V]` — `FromMember(member: V)` / `FromLonLat(coordinates: GeoCoordinates)`; the member type is threaded only through the case that needs it, as `ByLex[V]` is in `ZRange`.
- `GeoShape` — `ByRadius(radius, unit)` / `ByBox(width, height, unit)`.

So an area with no origin, both origins at once, or a box with a stray radius are all unrepresentable. `GeoUnit` (`Meters`/`Kilometers`/`Miles`/`Feet`) is shared across `geoDist`, `geoSearch`, and `geoSearchStore` under the same domain-primitive exception ADR-0011 carves out for `ListSide`. `GeoCount(count, any)` bundles `COUNT` with its `ANY` modifier so `ANY` without `COUNT` cannot be expressed.

## Considered Options

- **Binary bare/enriched split + `GeoSearchResult` with `Option` fields (chosen)** — keeps the members-only path as clean as `zRange`, collapses the eight-way flag explosion into one record. Matches Lettuce's `GeoWithin` and go-redis's `GeoSearch`/`GeoSearchLocation` split.
- **A builder per reply shape (strict ADR-0011)** — up to eight builders; untenable for three combinable flags.
- **Single builder always returning `GeoSearchResult[V]`** — forces `.member` unwrapping on the common members-only case and hands back all-`None` records, abandoning the clean bare path `zRange` establishes.
