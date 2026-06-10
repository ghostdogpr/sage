package sage.client.internal

import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.protocol.Frame

class ClientCacheSpec extends munit.FunSuite {

  private def key(s: String): Bytes                                       = Bytes.utf8(s)
  private def frame(s: String): Frame                                     = Frame.BulkString(Bytes.utf8(s))
  private def collector(): (Try[Frame] => Unit, () => Option[Try[Frame]]) = {
    var slot: Option[Try[Frame]] = None
    ((r: Try[Frame]) => slot = Some(r), () => slot)
  }

  test("first miss fetches, a concurrent miss waits, and the stored reply reaches both") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val (w2, get2) = collector()
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, w1), ClientCache.Fetch)
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, w2), ClientCache.Wait)
    cache.store(cmd, Vector(key("foo")), frame("bar"), 0L, 1000L)
    assertEquals(get1(), Some(Success(frame("bar"))))
    assertEquals(get2(), Some(Success(frame("bar"))))
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()), ClientCache.Hit(frame("bar")))
  }

  test("absolute TTL: a hit before expiry, a refetch at expiry") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    cache.store(cmd, Vector(key("foo")), frame("bar"), 0L, 1000L)
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 999L, _ => ()), ClientCache.Hit(frame("bar")))
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 1000L, _ => ()), ClientCache.Fetch)
  }

  test("an invalidation for a tracked key evicts every entry that touched it") {
    val cache = new ClientCache(1024)
    val get   = key("GET foo")
    val range = key("GETRANGE foo 0 1")
    cache.store(get, Vector(key("foo")), frame("bar"), 0L, 10000L)
    cache.store(range, Vector(key("foo")), frame("ba"), 0L, 10000L)
    cache.invalidate(key("foo"))
    assertEquals(cache.acquire(get, Vector(key("foo")), 0L, _ => ()), ClientCache.Fetch)
    assertEquals(cache.acquire(range, Vector(key("foo")), 0L, _ => ()), ClientCache.Fetch)
  }

  test("flush drops the whole cache") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    cache.store(cmd, Vector(key("foo")), frame("bar"), 0L, 10000L)
    cache.flush()
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()), ClientCache.Fetch)
  }

  test("an invalidation mid-flight delivers the reply but does not cache it") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, w1), ClientCache.Fetch)
    cache.invalidate(key("foo"))                                                         // arrives before the fetch completes
    cache.store(cmd, Vector(key("foo")), frame("bar"), 0L, 10000L)
    assertEquals(get1(), Some(Success(frame("bar"))))                                    // waiter still gets the value
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()), ClientCache.Fetch) // but it was not stored
  }

  test("a failed fetch reaches every waiter and stores nothing") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val boom       = new RuntimeException("boom")
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, w1), ClientCache.Fetch)
    cache.fail(cmd, boom)
    assertEquals(get1(), Some(Failure(boom)))
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()), ClientCache.Fetch)
  }

  test("the bytes cap evicts the least-recently-used entry") {
    val cache = new ClientCache(100) // each 50-byte value is ~66 bytes stored, so two together exceed the cap
    val big   = frame("x" * 50)
    cache.store(key("GET a"), Vector(key("a")), big, 0L, 10000L)
    cache.store(key("GET b"), Vector(key("b")), big, 0L, 10000L)
    assertEquals(cache.acquire(key("GET a"), Vector(key("a")), 0L, _ => ()), ClientCache.Fetch) // evicted
    assertEquals(cache.acquire(key("GET b"), Vector(key("b")), 0L, _ => ()), ClientCache.Hit(big))
  }
}
