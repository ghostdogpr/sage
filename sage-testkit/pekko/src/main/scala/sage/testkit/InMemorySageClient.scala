package sage.testkit

import sage.backend.SageClient
import sage.client.inmemory.InMemoryClient

/**
  * Creates a [[sage.backend.SageClient]] backed by an in-memory keyspace, for tests that should not need a Redis server. The client
  * supports the string, key, hash, list, set, sorted-set, stream, bitmap, HyperLogLog and geo commands, expiry, blocking commands,
  * pipelines, transactions, pub/sub, distributed locks and rate limiters. Other commands fail with a `ServerError` whose code is `ERR`.
  */
object InMemorySageClient {

  /**
    * Creates a client with its own empty keyspace. It reads time from the backend's clock, so a test clock of the effect system controls
    * expiry.
    */
  def apply(): SageClient = new SageClient.Lowered(InMemoryClient())

  /**
    * Creates a client with its own empty keyspace that reads time from `clock`, such as a [[TestClock.Manual]] clock that the test
    * advances.
    */
  def apply(clock: TestClock): SageClient = new SageClient.Lowered(InMemoryClient(clock))

  /**
    * Creates a client of `server`'s keyspace. Clients of the same server see each other's writes and pub/sub messages.
    */
  def apply(server: InMemoryServer): SageClient = new SageClient.Lowered(InMemoryClient(server))
}
