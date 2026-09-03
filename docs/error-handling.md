# Error handling

Every Sage failure is a `SageException` in a sealed hierarchy that you can match exhaustively. ZIO and Kyo include `SageException` in the error type as `IO[SageException, *]` and `Abort[SageException]`. Cats Effect raises the exception in `IO`, Ox throws it, and Pekko returns a failed `scala.concurrent.Future`.

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
| `TimedOut(message)` | A pooled connection wait exceeded `dedicatedPool.acquireTimeout`, a topology probe exceeded `connectTimeout`, or distributed lock acquisition exceeded its wait or lease budget. |
| `LockLost(message)` | A distributed lock expired, changed owner, or could not confirm renewal or release within its deadline. Sage attempts to cancel the protected body. Cancellation depends on the backend and cannot undo completed work. |
| `TransactionDiscarded(message)` | A transaction was discarded server-side (`EXECABORT`); nothing ran. |
| `NotCacheable(message)` | `cached` was given a command that cannot be safely cached. |
| `InvalidArgument(message)` | A programming error, rejected before any server call: an invalid configuration or rate-limit policy, a blocking command inside a pipeline or transaction, or a command a cluster client cannot route as written. |

Regular commands have no per-command timeout. Use your backend's timeout combinator to bound their duration. See [Distributed locks](/distributed-locks) for lock deadlines and cancellation behavior.

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

Sage does not retry a lost command or queue commands while disconnected. See [What happens when the connection drops?](/faq#what-happens-when-the-connection-drops). Use `mayHaveExecuted` and the command's idempotency to decide whether to retry.

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

Retries share the cluster's `maxRedirects` limit and use a short random delay. If the condition continues, Sage returns the original reply as a `ServerError` with its original code. A read first tries its next [`ReadFrom`](/configuration#read-routing) candidate. A refusing replica therefore costs one hop when the master or another replica can serve the read.

Sage does not retry `-CLUSTERDOWN` for [commands that run on every master](/configuration#commands-that-run-on-every-master), because the failover may have changed the master set. Retry the command yourself. The command does not run atomically across masters, so a master that completed the first attempt runs it again. Repetition is safe for reads, `SCRIPT LOAD`, and the `FLUSH` family. `FUNCTION` mutations reject a second run unless you pass `replace = true`, `RestorePolicy.Replace`, or `RestorePolicy.Flush`.

## How each backend reports failures

Each ecosystem reports the same `SageException` through its normal failure channel. ZIO and Kyo also include it in the error type. In those backends, another exception is a defect, such as a ZIO die or Kyo `Panic`.

- ZIO returns a failed `IO[SageException, *]`. Recover with `catchAll` or `catchSome`.
- Cats Effect raises the exception in `IO`. Recover with `handleErrorWith` or `recoverWith` and match on `SageException`.
- Kyo returns an `Abort[SageException]`. Handle it with the `Abort` combinators.
- Ox throws the exception in direct style. Handle it with `try` and `catch`.
- Pekko returns a failed `scala.concurrent.Future`. Recover with `recover` or `recoverWith`.
