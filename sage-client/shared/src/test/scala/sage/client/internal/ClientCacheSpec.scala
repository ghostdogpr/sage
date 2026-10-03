package sage.client.internal

import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.commands.{Command, Execution}
import sage.protocol.Frame

class ClientCacheSpec extends munit.FunSuite {

  private def key(s: String): Bytes                                                                                = Bytes.utf8(s)
  private def frame(s: String): Frame                                                                              = Frame.BulkString(Bytes.utf8(s))
  private def collector(): (Try[Frame] => Unit, () => Option[Try[Frame]])                                          = {
    var slot: Option[Try[Frame]] = None
    ((r: Try[Frame]) => slot = Some(r), () => slot)
  }
  private def hitFrame(acquired: ClientCache.Acquire): Frame                                                       = acquired match {
    case ClientCache.Acquire.Hit(frame, _) => frame
    case other                             => fail(s"expected a hit, got $other")
  }
  private def fetching(acquired: ClientCache.Acquire): ClientCache.Fetching                                        = acquired match {
    case ClientCache.Acquire.Fetch(ticket) => ticket
    case other                             => fail(s"expected a fetch, got $other")
  }
  private def assertFetch(acquired: ClientCache.Acquire): Unit                                                     = fetching(acquired): Unit
  // seed an entry through the acquire/store path a cache miss takes
  private def seed(cache: ClientCache, cmd: Bytes, tracked: Bytes, value: Frame, now: Long, ttlMillis: Long): Unit =
    cache.store(fetching(cache.acquire(cmd, Vector(tracked), now, _ => ())), value, now, ttlMillis)

  test("a command whose declared key positions fall outside its arguments is not cacheable") {
    val decode: Frame => Either[sage.SageException.DecodeError, Frame] = Right(_)
    assert(Client.cacheable(Command("GET", Vector(0), Vector(key("k")), decode, isReadOnly = true, cacheable = true)))
    assert(!Client.cacheable(Command("GET", Vector(5), Vector(key("k")), decode, isReadOnly = true, cacheable = true)))
  }

  test("a blocking command is not cacheable, since a cached read runs on the shared multiplexed connection") {
    val decode: Frame => Either[sage.SageException.DecodeError, Frame] = Right(_)
    val blocking                                                       = Command("BLPOP", Vector(0), Vector(key("k"), key("0")), decode, Execution.Blocking, isReadOnly = true, cacheable = true)
    assert(!Client.cacheable(blocking))
  }

  test("first miss fetches, a concurrent miss waits, and the stored reply reaches both") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val (w2, get2) = collector()
    val ticket     = fetching(cache.acquire(cmd, Vector(key("foo")), 0L, w1))
    assertEquals(cache.acquire(cmd, Vector(key("foo")), 0L, w2), ClientCache.Acquire.Wait)
    cache.store(ticket, frame("bar"), 0L, 1000L)
    assertEquals(get1(), Some(Success(frame("bar"))))
    assertEquals(get2(), Some(Success(frame("bar"))))
    assertEquals(hitFrame(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ())), frame("bar"))
  }

  test("absolute TTL: a hit before expiry, a refetch at expiry") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    seed(cache, cmd, key("foo"), frame("bar"), 0L, 1000L)
    assertEquals(hitFrame(cache.acquire(cmd, Vector(key("foo")), 999L, _ => ())), frame("bar"))
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 1000L, _ => ()))
  }

  test("an invalidation for a tracked key evicts every entry that touched it") {
    val cache = new ClientCache(1024)
    val get   = key("GET foo")
    val range = key("GETRANGE foo 0 1")
    seed(cache, get, key("foo"), frame("bar"), 0L, 10000L)
    seed(cache, range, key("foo"), frame("ba"), 0L, 10000L)
    cache.invalidate(key("foo"))
    assertFetch(cache.acquire(get, Vector(key("foo")), 0L, _ => ()))
    assertFetch(cache.acquire(range, Vector(key("foo")), 0L, _ => ()))
  }

  test("flush drops the whole cache") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    seed(cache, cmd, key("foo"), frame("bar"), 0L, 10000L)
    cache.flush()
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()))
  }

  test("a hit taken before a flush is retired, and looking it up again fetches from the server") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    seed(cache, cmd, key("foo"), frame("bar"), 0L, 10000L)
    val hit   = cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()).asInstanceOf[ClientCache.Acquire.Hit]
    assert(cache.isCurrent(hit))
    cache.flush()
    assert(!cache.isCurrent(hit), "a hit retired by a flush must not be delivered")
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()))
  }

  test("a hit taken after a flush is current") {
    val cache = new ClientCache(1024)
    val cmd   = key("GET foo")
    cache.flush()
    seed(cache, cmd, key("foo"), frame("bar"), 0L, 10000L)
    val hit   = cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()).asInstanceOf[ClientCache.Acquire.Hit]
    assert(cache.isCurrent(hit))
  }

  test("an invalidation mid-flight delivers the reply but does not cache it") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val ticket     = fetching(cache.acquire(cmd, Vector(key("foo")), 0L, w1))
    cache.invalidate(key("foo"))                                     // arrives before the fetch completes
    cache.store(ticket, frame("bar"), 0L, 10000L)
    assertEquals(get1(), Some(Success(frame("bar"))))                // waiter still gets the value
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ())) // but it was not stored
  }

  test("a flush mid-flight (MOVED, topology change, dropped connection) delivers the reply but does not cache it") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val ticket     = fetching(cache.acquire(cmd, Vector(key("foo")), 0L, w1))
    cache.flush()
    cache.store(ticket, frame("bar"), 0L, 10000L)
    assertEquals(get1(), Some(Success(frame("bar"))))
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()))
  }

  test("a failed fetch reaches every waiter and stores nothing") {
    val cache      = new ClientCache(1024)
    val cmd        = key("GET foo")
    val (w1, get1) = collector()
    val boom       = new RuntimeException("boom")
    val ticket     = fetching(cache.acquire(cmd, Vector(key("foo")), 0L, w1))
    cache.fail(ticket, boom)
    assertEquals(get1(), Some(Failure(boom)))
    assertFetch(cache.acquire(cmd, Vector(key("foo")), 0L, _ => ()))
  }

  test("the bytes cap evicts the least-recently-used entry") {
    val cache = new ClientCache(100) // each 50-byte value is ~66 bytes stored, so two together exceed the cap
    val big   = frame("x" * 50)
    seed(cache, key("GET a"), key("a"), big, 0L, 10000L)
    seed(cache, key("GET b"), key("b"), big, 0L, 10000L)
    assertFetch(cache.acquire(key("GET a"), Vector(key("a")), 0L, _ => ())) // evicted
    assertEquals(hitFrame(cache.acquire(key("GET b"), Vector(key("b")), 0L, _ => ())), big)
  }
}
