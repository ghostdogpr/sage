# Commands & codecs

Every Redis command is available two ways: as a method on the client (`client.get`, `client.incr`), and as a plain value built from the `Commands` facade (`Commands.get`, `Commands.incr`). The methods are thin sugar; the values are the foundation that pipelines, transactions, and reuse are built on.

## Commands as values

A `Command[Out]` is a pure description of one server command: its name, its arguments, the routing metadata, and a typed decoder for the reply. The client's per-command methods delegate to a single `run`, so these two lines do exactly the same thing:

::: code-group

```scala [Ox]
// per-command sugar
val greeting = client.get[String, String]("greeting")

// the same command, built as a value and run explicitly
val same = client.run(Commands.get[String, String]("greeting"))
```

```scala [ZIO · Cats Effect · Kyo]
for {
  greeting <- client.get[String, String]("greeting")
  // the same command, built as a value and run explicitly
  same     <- client.run(Commands.get[String, String]("greeting"))
} yield (greeting, same)
```

:::

Because a `Command` is just a value, you can hold it, pass it around, and reuse it. That is the same value a [pipeline or transaction](/pipelines-transactions) composes, so anything you can run on its own you can also batch.

## Command families

Commands mirror the server's documented groups, and the methods are named one-for-one with the Redis commands they issue:

- **Strings** (`get`, `set`, `incr`, `incrBy`, `append`, …)
- **Keys** (`del`, `exists`, `expire`, `ttl`, `scan`, …)
- **Hashes** (`hSet`, `hGet`, `hGetAll`, …)
- **Lists** (`lPush`, `rPush`, `lRange`, `blPop`, …)
- **Sets** and **Sorted sets** (`sAdd`, `sMembers`, `zAdd`, `zRange`, …)
- **HyperLogLog**, **Bitmaps**, **Geo**, **Streams**
- **Pub/Sub**, **Scripting**, **Functions**
- **Server**, **Connection**, **ACL**

The full surface lives on the client and on `Commands`; the API docs list every method with its signature.

## Typed keys and values

Keys and values are typed. The type parameters select which codec converts them to and from wire bytes:

```scala
client.set("user:1", 42)                    // value typed as Int
val n = client.get[String, Int]("user:1")   // key String, value Int
```

There are two separate typeclasses, by design:

- `KeyCodec[A]` for **key and hash-field** positions (identifiers into the keyspace or a hash).
- `ValueCodec[A]` for **payloads**.

They are deliberately unrelated, which keeps `given` resolution unambiguous and lets key positions carry the cluster-slot hashing that value positions do not need.

### Built-in codecs

| Type | `ValueCodec` | `KeyCodec` |
| --- | :---: | :---: |
| `String` (UTF-8) | yes | yes |
| `Int`, `Long` | yes | yes |
| `Bytes`, `Array[Byte]` | yes | yes |
| `Double`, `Float` | yes | no |
| `Boolean` | yes | no |

`Double`, `Float`, and `Boolean` are intentionally missing as key codecs: their formatting is representation-sensitive, and two writers must never silently address different keys or fields.

All built-in codecs **decode strictly**. Bytes that are not the type's canonical form fail with a `DecodeError` rather than being coerced: `"x"` is not a `Long`, and `"2"` is not a `Boolean`.

## Writing your own codec

Any type with a `ValueCodec` rides over the wire like a built-in. Build one from an existing codec with `imap` (a total, lossless mapping) or `emap` (a mapping whose decode can fail). Returning `Left` on bad input keeps the same strict, no-coercion contract.

A newtype is the `imap` case:

```scala
final case class UserId(value: Long)

given KeyCodec[UserId] = KeyCodec[Long].imap(UserId(_))(_.value)
```

A type whose decode is partial is the `emap` case. Here `User` encodes as `name|age` and rejects anything that is not that shape:

```scala
final case class User(name: String, age: Int)

object User {
  given ValueCodec[User] =
    ValueCodec[String].emap { raw =>
      raw.lastIndexOf('|') match {
        case -1 => Left(SageException.DecodeError("User(name|age)", raw))
        case i  =>
          raw.drop(i + 1).toIntOption
            .map(User(raw.take(i), _))
            .toRight(SageException.DecodeError("User(name|age)", raw))
      }
    }(user => s"${user.name}|${user.age}")
}
```

With that `given` in scope, a `User` is read and written exactly like a `String`:

```scala
client.set("user:ada", User("Ada", 36))
val ada = client.get[String, User]("user:ada") // Some(User("Ada", 36))
```

You can also build a codec from scratch with `ValueCodec.from` (or `KeyCodec.from`), supplying an encode function and a decode that returns `Either`.

## Next steps

- [Pipelines & transactions](/pipelines-transactions) compose `Command` values into one round-trip
- [Client-side caching](/client-side-caching) opts individual reads into a local cache
