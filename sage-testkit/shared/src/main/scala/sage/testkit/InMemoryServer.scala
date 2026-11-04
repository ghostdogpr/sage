package sage.testkit

import sage.client.inmemory.{Executor, Store}

/**
  * One in-memory keyspace that several clients can share, like several connections to the same Redis server. Clients created from the same
  * server see each other's writes and receive each other's pub/sub messages. Pass it to `InMemorySageClient(server)`.
  *
  * Without a clock, clients read time from the backend's own clock. With one, every client of the server reads time from it.
  */
final class InMemoryServer private (private[sage] val executor: Executor, private[sage] val clock: Option[TestClock])

object InMemoryServer {

  /**
    * Creates an empty server whose clients read time from the backend's clock.
    */
  def apply(): InMemoryServer = new InMemoryServer(new Executor(new Store), None)

  /**
    * Creates an empty server whose clients read time from `clock`.
    */
  def apply(clock: TestClock): InMemoryServer = new InMemoryServer(new Executor(new Store), Some(clock))
}
