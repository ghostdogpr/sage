# Error handling

Every Sage failure is a `SageException`, part of a sealed hierarchy that you can match exhaustively. ZIO and Kyo include `SageException` in the error type (`IO[SageException, *]`, `Abort[SageException]`). Cats Effect, Ox, and Pekko report the same exception through their usual untyped error handling: a raised `IO`, a thrown exception, or a failed `scala.concurrent.Future`.

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
| `InvalidArgument(message)` | A programming error, rejected before any server call: an invalid configuration or rate-limit policy, a blocking command inside a pipeline or transaction, or a command a cluster client cannot route as written. |

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

The `mayHaveExecuted` flag on `ConnectionLost` tells you whether a retry is safe:

- `false` means the command was never sent, so retrying is always safe.
- `true` means it was already in flight when the connection dropped, so the server may or may not have applied it. A non-idempotent command (an `INCR`, an `LPUSH`) is then not safe to blindly retry; an idempotent one (a `SET` to a fixed value) is.

Sage does not retry a lost command for you, and it does not queue commands while disconnected (see [What happens when the connection drops?](/faq#what-happens-when-the-connection-drops)). This flag gives you what you need to decide.

::: warning
When `mayHaveExecuted` is `true`, do not blindly retry a non-idempotent command: it may already have run. Retry only when the command is idempotent, or make it so first.
:::

## Refusals Sage retries for you

Some replies mean the server rejected the command *before* running it for a temporary reason. Sage retries these commands because the first attempt was not executed.

| Reply | What it means |
| --- | --- |
| `-TRYAGAIN` | A multi-key command whose keys straddle a slot being migrated. |
| `-CLUSTERDOWN` | The cluster is mid-failover, or the slot is not served right now. |
| `-LOADING` | The node is still loading its dataset. |
| `-MASTERDOWN` | A replica cut off from its master, running with `replica-serve-stale-data no`. |

Retries are bounded and spaced by a short random delay, sharing the cluster's `maxRedirects` budget. If the condition outlasts it, the original reply
is returned as a `ServerError` with its original code. A read tries its next [`ReadFrom`](/configuration#read-routing) candidate first, so a refusing replica
costs one hop when the master or another replica can serve it.

The exception is [commands that run on every master](/configuration#commands-that-run-on-every-master): Sage returns `-CLUSTERDOWN` to you because
the failover may have changed the set of masters. Retry the command yourself, keeping in mind that it does not run atomically across masters. Masters
that already ran the command run it again. That is harmless for reads, `SCRIPT LOAD`, and the `FLUSH` family, but the `FUNCTION` mutations refuse a
second run unless you pass `replace = true` (or `RestorePolicy.Replace` / `RestorePolicy.Flush`).

## How each backend reports failures

The same `SageException` is delivered through each ecosystem's normal failure channel. ZIO and Kyo carry it in the type as well, so on those two a non-`SageException` is a defect (a ZIO die, a Kyo `Panic`) rather than a typed failure:

- **ZIO**: a failed `IO[SageException, *]`; recover with `catchAll` / `catchSome`, which hand you a `SageException` directly.
- **Cats Effect**: a raised `IO`; recover with `handleErrorWith` / `recoverWith` and match the `SageException`.
- **Kyo**: an `Abort[SageException]`; handle with the `Abort` combinators.
- **Ox**: thrown in direct style; handle with an ordinary `try`/`catch`.
- **Pekko**: a failed `scala.concurrent.Future`; recover with `recover` / `recoverWith`.
