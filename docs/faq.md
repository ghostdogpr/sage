# FAQ

## How does Sage perform?

Commands from every fiber share one auto-pipelined connection per node. Sage can combine concurrent commands into a single socket write and one round trip instead of sending each one separately. I/O runs on virtual threads with blocking reads and writes. Reading replies does not block new commands from being written, and the RESP3 parser and codecs decode replies directly into your types.

In concurrent workloads, Sage matches or outperforms established Scala clients. You can run [the benchmarks](https://github.com/ghostdogpr/sage/tree/main/benchmarks) against a real server. Results vary by version and hardware, so the repository does not publish fixed numbers.

## Which backend artifact should I use?

One per Scala stack, all sharing the same runtime:

- ZIO: `sage-client-zio`
- Cats Effect: `sage-client-ce`
- Kyo: `sage-client-kyo`
- Ox: `sage-client-ox`
- Pekko: `sage-client-pekko`

`sage-core` comes in transitively, so you depend on the backend artifact only. See [Getting started](/getting-started).

## Does every command open or borrow a connection?

No. Ordinary commands are auto-pipelined onto one multiplexed connection per node, shared by every fiber, with replies matched in order. Commands that hold per-connection state or block (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, and the like) temporarily use a dedicated connection. Pub/sub uses its own subscription connection. The [Getting started](/getting-started) "how it works" aside covers this.

## Redis or Valkey? Which versions?

Both. Sage targets RESP3 and modern Redis 8+ and Valkey 8+, where every command it exposes is available. It connects to any RESP3-capable server (Redis 6.0+), so an older server works for the subset of commands that version supports; commands added later (hash-field TTL in 7.4, `HGETEX`/`HGETDEL`/`HSETEX` in 8.0) simply error on a server that predates them.

## What Scala and JDK versions are required?

Scala 3.3.x LTS and later, on JDK 21 or newer.

## Is Scala.js or Scala Native supported?

No. The core is JVM-only.

## Is Redis Sentinel supported?

No, Sentinel is out of scope. Sage supports standalone, cluster, and master-replica deployments; see [Configuration](/configuration).

## Can I run Lua scripts or server-side functions?

Yes. `client.eval` (with `client.scriptLoad` / `client.evalSha` for cached scripts) runs Lua; `client.functionLoad`, `client.fCall`, `client.functionList`, and `client.functionDelete` manage and call server-side functions grouped into libraries. Read-only `*Ro` variants exist for the eligible commands. In a cluster, script and function management is routed to every master automatically. These replies come back as a raw `Frame`, which you decode with the strict helpers (for example `reply.asLong`).

## What happens when the connection drops?

Sage fails fast and reconnects in the background with exponential backoff. It does not buffer commands while disconnected. A command in flight when the connection drops fails with `ConnectionLost`, whose `mayHaveExecuted` flag tells you whether retrying is safe (see [Error handling](/error-handling)). A watchdog detects and replaces connections that stop responding. You can configure reconnect and watchdog behavior on [`SageConfig`](/configuration).
