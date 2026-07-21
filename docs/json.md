# JSON

Sage supports the `JSON.*` command surface shipped by Redis 8, which has JSON built in (RedisJSON), and by the Valkey Bundle (the valkey-json module). Documents are stored server-side and addressed with JSONPath expressions. Sage takes no JSON-library dependency: a document is raw JSON text on the wire, and any structured typing is your own codec.

## Paths

Every JSON command locates values with a `JsonPath`. Sage supports the JSONPath dialect (expressions beginning with `$`), and a path defaults to the document root `$`. Location commands always send an explicit path; `jsonGet` alone omits it when you pass none, to return the whole document unwrapped (see below).

```scala
JsonPath.root          // $
JsonPath("$.name")     // a field
JsonPath("$.items[0]") // an array element
JsonPath("$..price")   // every price, at any depth
```

A JSONPath can match more than one location, so the location commands (`jsonType`, `jsonArrLen`, `jsonStrLen`, `jsonArrAppend`, `jsonToggle`, and the rest) return a `Vector` with one entry per matched location, `None` where a match exists but holds the wrong type for the command. A path that matches nothing returns an empty `Vector`, so prefer `.headOption` over `.head` when you expect a single match.

The legacy dot dialect is not modeled. If you need it, send a raw command.

## Documents are raw JSON

A value you write is raw JSON text. The built-in `String` codec passes it through unchanged, so you supply valid JSON yourself: a scalar string is `"quoted"`, and objects and arrays are their JSON forms.

::: code-group

```scala [Ox]
client.jsonSet("user:1", JsonPath.root, """{"name":"Ada","age":36,"tags":["a","b"]}""")
val name = client.jsonGet[String]("user:1", JsonPath("$.name")) // Some("[\"Ada\"]")
val age  = client.jsonGet[String]("user:1", JsonPath("$.age"))  // Some("[36]")
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _    <- client.jsonSet("user:1", JsonPath.root, """{"name":"Ada","age":36,"tags":["a","b"]}""")
  name <- client.jsonGet[String]("user:1", JsonPath("$.name"))
  age  <- client.jsonGet[String]("user:1", JsonPath("$.age"))
} yield (name, age)
```

:::

`jsonGet` returns the value as one JSON text blob. With no path it returns the whole document unwrapped, so a typed codec decodes it directly; a JSONPath wraps its matches in a JSON array (so `$.name` reads back as `["Ada"]`), and several paths merge into one path-keyed object.

To decode documents into your own types, bring a `ValueCodec` built from your JSON library. Its encode must emit valid JSON and its decode parses it back. With circe, one codec covers every type circe handles:

```scala
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import sage.SageException.DecodeError

given [A](using io.circe.Decoder[A], io.circe.Encoder[A]): ValueCodec[A] =
  ValueCodec.string.emap(s => decode[A](s).left.map(DecodeError.fromThrowable))(_.asJson.noSpaces)

case class User(name: String, age: Int)

client.jsonSet("user:1", JsonPath.root, User("Ada", 36))
client.jsonGet[User]("user:1")                           // Some(User("Ada", 36)): whole document, unwrapped
client.jsonGet[Vector[Int]]("user:1", JsonPath("$.age")) // Some(Vector(36)): a JSONPath wraps matches in an array
```

Sage takes no JSON dependency of its own; the codec is entirely yours.

`jsonSet` models the `NX` and `XX` conditions; other server options (the `jsonGet` formatting hints `INDENT`, `NEWLINE`, `SPACE`, and newer `JSON.SET` storage hints) are not modeled. Reach them with a raw command.

## Working with the document

Numbers, strings, booleans, arrays, and objects each have their commands. All of the location commands return per-match `Vector`s.

::: code-group

```scala [Ox]
client.jsonSet("doc", JsonPath.root, """{"n":1,"s":"ab","flag":false,"xs":[1,2,3]}""")
client.jsonNumIncrBy("doc", JsonPath("$.n"), 5)        // Vector(Some(6.0))
client.jsonStrAppend("doc", JsonPath("$.s"), "\"c\"")  // Vector(Some(3))
client.jsonToggle("doc", JsonPath("$.flag"))           // Vector(Some(true))
client.jsonArrAppend("doc", JsonPath("$.xs"), "4")     // Vector(Some(4))
client.jsonArrLen("doc", JsonPath("$.xs"))             // Vector(Some(4))
client.jsonType("doc", JsonPath("$.n"))                // Vector(Some(JsonType.Integer))
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _ <- client.jsonSet("doc", JsonPath.root, """{"n":1,"s":"ab","flag":false,"xs":[1,2,3]}""")
  _ <- client.jsonNumIncrBy("doc", JsonPath("$.n"), 5)
  _ <- client.jsonStrAppend("doc", JsonPath("$.s"), "\"c\"")
  _ <- client.jsonToggle("doc", JsonPath("$.flag"))
  _ <- client.jsonArrAppend("doc", JsonPath("$.xs"), "4")
  n <- client.jsonArrLen("doc", JsonPath("$.xs"))
  t <- client.jsonType("doc", JsonPath("$.n"))
} yield (n, t)
```

:::

## Multiple keys and the cluster

`jsonMGet` reads the same path from several documents at once, returning one `Option` per key:

```scala
client.jsonMGet[String](JsonPath("$.name"))("user:1", "user:2", "user:3")
// Vector(Some("[\"Ada\"]"), Some("[\"Lin\"]"), None)
```

`jsonMSet` sets several documents atomically:

```scala
client.jsonMSet(("user:1", JsonPath.root, """{"n":"Ada"}"""), ("user:2", JsonPath.root, """{"n":"Lin"}"""))
```

In a cluster these two commands differ by design. `jsonMGet` is transparently split across slots and its results are recombined, so its keys may live anywhere. `jsonMSet` is atomic, so a call whose keys span slots is rejected rather than partially applied. To set several documents atomically, co-locate their keys with a hash tag so they share one slot:

```scala
client.jsonMSet(("{acct:9}:profile", JsonPath.root, "{}"), ("{acct:9}:prefs", JsonPath.root, "{}"))
```

## Redis and Valkey differences

The common surface behaves the same on both servers. A few points differ:

- `jsonMerge` (RFC 7386 merge) is available on Redis only; the Valkey Bundle 9.1.0 does not ship `JSON.MERGE` yet. It is present in the Sage API and works against Redis.
- The two servers frame the replies to `jsonType`, `jsonNumIncrBy`, and `jsonNumMultBy` differently on the wire. Sage decodes both into the same result type, so your code does not see the difference.
- `jsonSet` into a missing intermediate path that cannot be created returns `false` on Redis but raises a server error on Valkey.

Redis 8 ships JSON built in, but the stock `valkey` image carries no modules, so the tests run against JSON-capable images: `redis:8.8.0` and `valkey/valkey-bundle` (which adds valkey-json).
