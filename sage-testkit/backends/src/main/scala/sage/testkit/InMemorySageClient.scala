package sage.testkit

import sage.backend.SageClient
import sage.client.inmemory.InMemoryClient

/**
  * Creates a [[sage.backend.SageClient]] backed by an in-memory keyspace, for tests that should not need a Redis server. The client
  * supports the string, key, hash, list, set and sorted-set commands, expiry, blocking commands, pipelines, transactions, pub/sub,
  * distributed locks and rate limiters. Other commands fail with a `ServerError` whose code is `ERR`.
  */
object InMemorySageClient {

  /**
    * Creates a client of `server`'s keyspace, which by default is a new, empty one that reads time from the backend's clock, so a test
    * clock of the effect system controls expiry. Clients of the same server see each other's writes and pub/sub messages. To read time
    * from a [[TestClock]] clock that the test advances, pass `InMemoryServer(clock)`.
    */
  def apply(server: InMemoryServer = InMemoryServer()): SageClient = new SageClient.Lowered(InMemoryClient(server))
}
