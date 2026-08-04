# JSON

Sage supports the `JSON.*` commands, which store documents server-side and address them with JSONPath expressions. You need a server that provides them: Redis 8 has JSON built in, and on Valkey they come from the valkey-json module (shipped in the `valkey/valkey-bundle` image, not in the stock `valkey` one).

Sage does not depend on a JSON library. It sends and receives documents as raw JSON text, and you can use your own codec for typed values.

## Paths

Every JSON command locates values with a `JsonPath`, using the JSONPath dialect (expressions beginning with `$`). A path defaults to the document root `$`.

```scala
JsonPath.root          // $
JsonPath("$.name")     // a field
JsonPath("$.items[0]") // an array element
JsonPath("$..price")   // every price, at any depth
```

A JSONPath can match more than one location. Commands such as `jsonType`, `jsonArrLen`, `jsonStrLen`, `jsonArrAppend`, and `jsonToggle` return a `Vector` with one entry for each match. An entry is `None` when the matched value has the wrong type for the command. A path that matches nothing returns an empty `Vector`; use `.headOption` when you expect a single match.

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

`jsonGet` returns one block of JSON text. Without a path, it returns the complete document for a typed codec to decode. With one JSONPath, it wraps the matches in an array; for example, `$.name` returns `["Ada"]`. With several paths, it returns an object whose keys are the paths.

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

In a cluster, `jsonMGet` can read keys from different slots. Sage splits the request and combines the results. `jsonMSet` is atomic, so all keys must share one slot. Use a hash tag to place related keys in the same slot:

```scala
client.jsonMSet(("{acct:9}:profile", JsonPath.root, "{}"), ("{acct:9}:prefs", JsonPath.root, "{}"))
```

## Redis and Valkey differences

Most commands behave the same on both servers. There are a few differences:

- `jsonMerge` (RFC 7386 merge) works on Redis only. Valkey does not ship `JSON.MERGE` yet.
- `jsonSet` into a missing intermediate path that cannot be created returns `false` on Redis but fails with a server error on Valkey.
