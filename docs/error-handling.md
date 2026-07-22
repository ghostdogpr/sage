# Error handling

Every sage failure is a `SageException`, a single sealed hierarchy you can match exhaustively. On ZIO and Kyo the error channel is that `SageException` itself (`IO[SageException, *]`, `Abort[SageException]`), so the compiler holds you to handling it. On Cats Effect, Ox, and Pekko the same `SageException` arrives through the ecosystem's untyped failure channel: a raised `IO`, a thrown exception, a failed `scala.concurrent.Future`. The runtime value is always a `SageException`; ZIO and Kyo also put it in the type.

## The hierarchy

| Case | Meaning |
| --- | --- |
| `ProtocolError(message)` | Malformed RESP3 on the wire; the connection is discarded. |
| `DecodeError(expected, actual)` | A reply was well-formed but not the shape a decoder or codec required (the built-in codecs decode strictly). |
| `ServerError(code, detail)` | An error reply from the server. `code` is the leading token (`WRONGTYPE`, `NOSCRIPT`, `BUSYGROUP`, the generic `ERR`, …). |
| `ConnectionFailed(message)` | The initial connection could not be established (host unreachable, connection refused, or connect timeout). Distinct from `ConnectionLost`, which is a live connection dropping. |
| `ConnectionLost(mayHaveExecuted)` | The connection dropped around this command. |
| `NotConnected()` | The client was never started, or has been closed. |
| `UnsupportedServer(message)` | The server rejected `HELLO 3` (it predates RESP3, or is a RESP2-only proxy). |
| `TlsError(message)` | TLS could not be established (rejected certificate or unusable trust material). |
| `CrossSlot(message)` | An unsupported multi-key command or a transaction touched keys in more than one cluster slot. `MGET`, `MSET`, `EXISTS`, `DEL`, `UNLINK`, and `TOUCH` are transparently split outside transactions. |
| `TimedOut(message)` | A blocking command or transaction waited past `dedicatedPool.acquireTimeout` for a free pooled connection. Not a per-command timeout; bound a command's own duration with your backend's timeout combinator. |
| `TransactionDiscarded(message)` | A transaction was discarded server-side (`EXECABORT`); nothing ran. |
| `NotCacheable(message)` | `cached` was given a command that cannot be safely cached. |
| `InvalidArgument(message)` | An argument the API can never accept: an invalid configuration or rate-limit policy, a blocking command in a pipeline or transaction, or a command a cluster client cannot serve as routed (an all-masters command in a cluster pipeline, a cluster-wide result in a single-node transaction, a hand-built command it cannot route by key). A programming error, rejected before any server call. |

## Branching on the failure

Because the hierarchy is sealed and `ServerError` splits out the server's error code, you can match without parsing strings:

```scala
import sage.SageException.*

def classify(e: SageException): String = e match {
  case ServerError("WRONGTYPE", _) => "wrong type for this key"
  case ServerError(code, _)        => s"server error: $code"
  case DecodeError(expected, _)    => s"could not decode: wanted $expected"
  case ConnectionLost(true)        => "retry only if the command is idempotent"
  case ConnectionLost(false)       => "safe to retry, it was never sent"
  case CrossSlot(_)                => "keys span multiple cluster slots"
  case _                           => "other failure"
}
```

## Retrying after a connection loss

`ConnectionLost` carries a `mayHaveExecuted` flag, and it is the key to safe retries:

- `false` means the command was never sent, so retrying is always safe.
- `true` means it was already in flight when the connection dropped, so the server may or may not have applied it. A non-idempotent command (an `INCR`, an `LPUSH`) is then not safe to blindly retry; an idempotent one (a `SET` to a fixed value) is.

Sage does not retry for you, and it does not queue commands while disconnected (see [What happens when the connection drops?](/faq#what-happens-when-the-connection-drops)). This flag gives you what you need to decide.

::: warning
When `mayHaveExecuted` is `true`, do not blindly retry a non-idempotent command: it may already have run. Retry only when the command is idempotent, or make it so first.
:::

## How failures surface per backend

The same `SageException` is delivered through each ecosystem's normal failure channel. ZIO and Kyo carry it in the type as well, so on those two a non-`SageException` is a defect (a ZIO die, a Kyo `Panic`) rather than a typed failure:

- **ZIO**: a failed `IO[SageException, *]`; recover with `catchAll` / `catchSome`, which hand you a `SageException` directly.
- **Cats Effect**: a raised `IO`; recover with `handleErrorWith` / `recoverWith` and match the `SageException`.
- **Kyo**: an `Abort[SageException]`; handle with the `Abort` combinators.
- **Ox**: thrown in direct style; handle with an ordinary `try`/`catch`.
- **Pekko**: a failed `scala.concurrent.Future`; recover with `recover` / `recoverWith`.
