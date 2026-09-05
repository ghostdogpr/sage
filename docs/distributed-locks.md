# Distributed locks

`client.lock` coordinates work on the same resource across processes using Redis or Valkey. Sage acquires the lock before running your body, renews it while the body runs, and releases it when the body finishes.

```scala
import scala.concurrent.duration.*
import sage.*
import sage.backend.*

val locks = client.lock[String]()

locks.withLock("account:42", waitTimeout = 2.seconds) {
  updateAccount()
}
```

The body returns your backend's native effect. With Ox, it is a direct-style block. Construct the operation inside the block: an already-started `Future` may run before acquisition. Keep all protected work inside the returned effect. Wait for every child task before the body completes because detached work can continue after Sage releases the lock.

## Waiting for a lock

- `withLock(key, waitTimeout)(body)` waits for the lock and returns the body's result. Retries back off under contention to reduce server traffic. If acquisition times out, it raises `SageException.TimedOut`.
- `tryWithLock(key)(body)` does not wait for a busy lock. It returns `None` when busy, without evaluating the body, or `Some(result)` after successful execution and release. Sage retries temporary failures for up to about one second, bounded by the usable lease, then raises `SageException.TimedOut`.

`waitTimeout` must be positive. It covers acquisition, including server replies and retry delays, but does not limit the body's runtime. Cleanup can add up to one second before a timeout returns.

Sage retries connection failures, routing changes, and temporary server refusals within the acquisition wait. Other failures return immediately.

Sage never retries the body. Locks do not guarantee acquisition order. Acquiring the same key inside its own lock scope contends with the outer scope.

## Lease and namespace

The lease defaults to 30 seconds and must be at least 30 milliseconds. Sage renews it automatically, so the body can run longer than one lease.

```scala
val locks = client.lock[String](leaseDuration = 10.seconds, namespace = "account-updates")
```

Choose a lease comfortably longer than expected network latency and process pauses. A longer lease delays recovery after a holder crashes. Invalid lease or wait durations raise `SageException.InvalidArgument`.

All processes protecting the same resource must use the same deployment, namespace, and compatible key encoding. Keys support any `KeyCodec`. The default namespace is `lock`; reserve it for locks and give other application data separate namespaces.

The same code works with standalone, master-replica, and cluster clients. By default, replicated deployments wait for replicas to acknowledge acquisition and renewal. A slow or unavailable replica can therefore delay acquisition or cause a running lock to be lost.

You can favor availability over failover safety by disabling replica acknowledgement:

```scala
val locks = client.lock[String](replicaAcknowledgement = false)
```

Replicated deployments always require permission to run `ROLE`. The default acknowledgement mode also requires `WAIT`. Standalone clients do not run either command.

Lock checks share `dedicatedPool` with blocking commands and transactions. A full pool can prevent acquisition or renewal, even for different lock keys. Increasing `dedicatedPool.maxConnections` allows more of these operations to run concurrently.

## Failure and cancellation

If replicas do not acknowledge acquisition in time, Sage raises `SageException.TimedOut` without starting the body. Acquisition can also raise `SageException.LockLost` if the granting master changes role or the lease expires before the body starts. Sage retries temporary renewal failures while the usable lease remains. If it cannot recover, or loses ownership, the operation fails with `SageException.LockLost` and attempts to cancel the body. Server and connection errors propagate through your backend's normal failure channel. Cleanup preserves an existing body failure or cancellation.

Cancellation is cooperative and cannot undo completed work. Uninterruptible work can delay completion. Pekko uses `Future`, so its body may continue after lock loss even though Sage stops renewing the lock. A timed-out or cancelled acquisition can leave the lock held until its lease expires.

## Limits

Exclusivity depends on the lease remaining valid and the server retaining the lock. Replica acknowledgement reduces the risk of losing a lock during failover, but does not make Redis or Valkey strongly consistent. A long process pause, failover, or an evicted lock key can still allow overlapping work. Prevent eviction or deletion of active lock keys.

Sage does not provide fencing tokens or exactly-once execution. If correctness must survive those failures, the system receiving the changes must enforce ownership or provide transactional protection. See [Redis's distributed lock documentation](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) for the failure assumptions.
