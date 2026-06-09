# Pub/sub backpressure blocks the shared connection; the watchdog is backpressure-aware

Each subscription buffers into a bounded queue; when it fills, the reader draining the Subscription Connection blocks, the socket buffer fills, and TCP backpressures the publisher. Delivery is therefore lossless, paid for by a slow consumer stalling its *peer* subscriptions on the same connection. This is the deliberate scope of ADR-0003's isolation promise: pub/sub is isolated from command traffic (a separate connection), not consumer-from-consumer. The rejected alternatives were dropping on a full buffer (isolates peers but silently loses messages, hiding exactly the kind of mismatch a user wants surfaced) and a connection per subscription (forfeits the single-connection model and its lazy create/close-when-last lifecycle).

Blocking the one reader has a consequence the watchdog must account for: while the reader is parked on a full buffer it cannot process the watchdog's PONG, so an idle-PING liveness check would mistake deliberate backpressure for a dead connection and reconnect it — which resubscribes, refills the same buffer, and churns. The watchdog therefore skips its liveness kill while the reader is knowingly blocked on delivery; a connection backpressured by a slow consumer is alive by definition, not stalled by a half-open peer.

## Consequences

- A misbehaving consumer that never drains its stream halts delivery to every other subscription on the client until it drains or its scope closes. This is documented behavior, not a bug; striping subscriptions across connections is a possible later tuning knob, as for the Multiplexed Connection.
- The buffer bound is configurable (`PubSubConfig.bufferSize`): larger absorbs longer consumer pauses at higher memory cost, smaller backpressures sooner.
