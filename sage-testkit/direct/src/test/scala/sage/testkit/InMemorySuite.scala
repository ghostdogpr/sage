package sage.testkit

import java.util.concurrent.CompletableFuture

import scala.concurrent.duration.*

import sage.Bytes
import sage.backend.SageClient
import sage.client.inmemory.Executor
import sage.protocol.Frame

abstract class InMemorySuite extends munit.FunSuite {

  val clients: FunFixture[SageClient] = FunFixture(_ => InMemorySageClient(), _.close)

  val clocked: FunFixture[(SageClient, TestClock)] = FunFixture(
    _ => {
      val clock = TestClock()
      (InMemorySageClient(InMemoryServer(clock)), clock)
    },
    (client, _) => client.close
  )

  val shared: FunFixture[(InMemoryServer, SageClient, SageClient)] = FunFixture(
    _ => {
      val server = InMemoryServer()
      (server, InMemorySageClient(server), InMemorySageClient(server))
    },
    (_, first, second) => {
      first.close
      second.close
    }
  )
}

// runs a command line with raw arguments, which can be ones the typed API never sends
def raw(executor: Executor, command: String, now: Long = 0L): Frame = {
  val words = command.split(" ").toVector
  executor.execute(words.head, words.tail.map(Bytes.utf8), now)
}

def inBackground[A](body: => A): CompletableFuture[A] = {
  val result = new CompletableFuture[A]()
  Thread.startVirtualThread(() => result.complete(body): Unit): Unit
  result
}

// waits up to five seconds until `count` blocking commands or locks are waiting for a write on `server`
def awaitWaiting(server: InMemoryServer, count: Int): Unit = {
  val deadline = System.nanoTime() + 5.seconds.toNanos
  while (server.executor.waiting != count) {
    if (System.nanoTime() > deadline) throw new AssertionError(s"expected $count waiting commands, found ${server.executor.waiting}")
    Thread.`yield`()
  }
}
