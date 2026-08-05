# Commands & codecs

Every Redis command is available two ways: as a method on the client (`client.get`, `client.incr`), and as a value built with `Commands` (`Commands.get`, `Commands.incr`). Client methods are convenient for direct calls. Command values can be stored, reused, or included in pipelines and transactions.

## Commands as values

A `Command[Out]` describes a server command without running it. Client methods use `run` internally, so these two lines do exactly the same thing:

::: code-group

```scala [Ox]
// per-command sugar
val greeting = client.get[String]("greeting")

// the same command, built as a value and run explicitly
val same = client.run(Commands.get[String, String]("greeting"))
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  greeting <- client.get[String]("greeting")
  // the same command, built as a value and run explicitly
  same     <- client.run(Commands.get[String, String]("greeting"))
} yield (greeting, same)
```

:::

You can store a `Command`, pass it to a function, and reuse it. You can also include any command value in a [pipeline or transaction](/pipelines-transactions).

## Command families

Commands use the same groups and names as the Redis commands they send:

- **Strings** (`get`, `set`, `incr`, `incrBy`, `append`, …)
- **Keys** (`del`, `exists`, `expire`, `ttl`, `scan`, …)
- **Hashes** (`hSet`, `hGet`, `hGetAll`, …)
- **Lists** (`lPush`, `rPush`, `lRange`, `blPop`, …)
- **Sets** and **Sorted sets** (`sAdd`, `sMembers`, `zAdd`, `zRange`, …)
- **HyperLogLog**, **Bitmaps**, **Geo**, **Streams**
- **Pub/Sub**, **Scripting**, **Functions**
- **Server**, **Connection**, **ACL**

Every command is available on both the client and `Commands`. The [API docs](https://javadoc.io/doc/com.github.ghostdogpr/sage-core_3/) list every method with its signature.

In a cluster, Sage uses a command's key to choose the correct slot. A command without a key either runs on every master or on a single node. See [commands that run on every master](/configuration#commands-that-run-on-every-master).

## Typed keys and values

Keys and values are typed, and a codec converts each to and from bytes. The **key type is fixed on the client**: the default `SageClient` uses `String` keys, so you only need to specify the value type when calling a command:

```scala
client.set("user:1", 42)            // value inferred as Int
val n = client.get[Int]("user:1")   // value Int; the key is the client's String
```

A read like `get` returns the value, so its type cannot be inferred and is named explicitly; a write infers the value from its argument and needs no type parameter.

Sage uses two separate typeclasses:

- `KeyCodec[A]` for **key and hash-field** positions (identifiers into the keyspace or a hash).
- `ValueCodec[A]` for **payloads**.

They are separate because keys need cluster-slot hashing, while values do not.

### Non-String keys

Redis keys are binary-safe, so any type with a `KeyCodec` (`Int`, `Long`, raw bytes, your own newtype) is a valid key. Use `as[K]` to view the client with a different key type. It reuses the same connection:

```scala
val binary = client.as[Array[Byte]]
binary.set(idBytes, 42)
val n = binary.get[Int](idBytes)
```

The client returned by `as[K]` uses `K` for keys in commands, pipelines, transactions, subscriptions, and streaming helpers (`scanAll`, `hScanAll`, and so on). You can also use it inside a transaction (`tx.as[Array[Byte]].get(k)`). When building commands for a pipeline, specify both type parameters (`Commands.get[K, V]`) because `Commands` does not have a fixed key type.

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

Define a `ValueCodec` to read and write your own types just like the built-in types. Build one from an existing codec with `imap` (a total, lossless mapping) or `emap` (a mapping whose decode can fail). Return `Left` when the input is invalid.

Use `imap` for a newtype:

```scala
final case class UserId(value: Long)

given KeyCodec[UserId] = KeyCodec[Long].imap(UserId(_))(_.value)
```

Use `emap` when decoding can fail. Here `User` encodes as `name|age` and rejects anything that does not have that format:

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
val ada = client.get[User]("user:ada") // Some(User("Ada", 36))
```

You can also build a codec from scratch with `ValueCodec.from` (or `KeyCodec.from`), supplying an encode function and a decode that returns `Either`.

## Next steps

- [Pipelines & transactions](/pipelines-transactions) compose `Command` values into one round-trip
- [Client-side caching](/client-side-caching) opts individual reads into a local cache
