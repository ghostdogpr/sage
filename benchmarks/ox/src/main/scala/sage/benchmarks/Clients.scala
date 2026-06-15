package sage.benchmarks

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters.*

import _root_.ox.{fork, supervised}
import io.lettuce.core.{RedisClient, RedisFuture}

import sage.*
import sage.client.{Endpoint, SageConfig, Topology}
import sage.ox.*

/**
  * The Ox cell's benchmark clients: sage (direct style) and raw Lettuce driven async/auto-pipelined — the JVM ceiling.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-ox" => new SageOxBench(host, port)
    case "lettuce" => new LettuceBench(host, port)
    case other     => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

/**
  * Every sage-ox method needs an ambient `Ox`, so the connection is opened on a holder fiber whose `supervised` scope stays alive for the
  * client's lifetime; each operation runs in its own transient `supervised` scope and shares the long-lived connection.
  */
final class SageOxBench(host: String, port: Int) extends BenchClient {

  @volatile private var client: SageClient = null
  private val ready                        = new CountDownLatch(1)
  private val shutdown                     = new CountDownLatch(1)

  private val holder = Thread.ofVirtual().start { () =>
    supervised {
      client = SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port))))
      ready.countDown()
      shutdown.await()
      try client.close
      catch { case _: Throwable => () }
    }
  }
  ready.await()

  def name: String = "sage-ox"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = supervised {
    (0 until count).foreach { i =>
      val _ = client.set(s"$prefix:$i", value)
    }
    (0 until fields).foreach { i =>
      val _ = client.hSet(hashKey, (s"f$i", value))
    }
  }

  def getAll(keys: Array[String], concurrency: Int): Long = supervised {
    Payloads
      .groups(keys, concurrency)
      .toList
      .map(g => fork(g.foldLeft(0L)((t, k) => t + client.get[String](k).fold(0L)(_.length.toLong))))
      .map(_.join())
      .sum
  }

  def setAll(keys: Array[String], value: String, concurrency: Int): Long = supervised {
    Payloads
      .groups(keys, concurrency)
      .toList
      .map(g =>
        fork(g.foldLeft(0L) { (n, k) =>
          val _ = client.set(k, value); n + 1
        })
      )
      .map(_.join())
      .sum
  }

  def mget(keys: Array[String]): Long = supervised(client.mGet[String](keys.head, keys.tail*).flatten.map(_.length.toLong).sum)

  def hgetall(key: String): Long = supervised(client.hGetAll[String, String](key).size.toLong)

  def close(): Unit = {
    shutdown.countDown()
    holder.join()
  }
}

/**
  * Lettuce driven async/auto-pipelined — the JVM ceiling: up to `concurrency` commands fired before awaiting, so the shared connection
  * coalesces them into few socket writes (the same multiplexing redis4cats gets for free, with no effect-system layer on top).
  */
final class LettuceBench(host: String, port: Int) extends BenchClient {

  private val client = RedisClient.create(s"redis://$host:$port")
  private val conn   = client.connect()
  private val async  = conn.async()

  def name: String = "lettuce"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val writes = (0 until count).map(i => async.set(s"$prefix:$i", value)) ++ (0 until fields).map(i => async.hset(hashKey, s"f$i", value))
    writes.foreach { f =>
      val _ = f.get()
    }
  }

  // sliding window: keep exactly `concurrency` futures in flight at all times — each completion fires the next key — so a slow lane never
  // barriers the others (a chunk-then-wait-all loop would, under-measuring the ceiling). The completion callbacks run on Lettuce's event loop.
  private def slidingWindow[T](keys: Array[String], concurrency: Int)(submit: String => RedisFuture[T])(score: T => Long): Long = {
    val n                = keys.length
    val width            = math.max(1, math.min(concurrency, n))
    val total            = new AtomicLong(0L)
    val nextIndex        = new AtomicInteger(0)
    val remaining        = new CountDownLatch(n)
    val failure          = new AtomicReference[Throwable]()
    def fireNext(): Unit = {
      val i = nextIndex.getAndIncrement()
      if (i < n) {
        try {
          val _ = submit(keys(i)).whenComplete { (v, t) =>
            if (t != null) { val _ = failure.compareAndSet(null, t) }
            else if (v != null) { val _ = total.addAndGet(score(v)) }
            remaining.countDown()
            fireNext()
          }
        } catch {
          case t: Throwable =>
            val _ = failure.compareAndSet(null, t)
            remaining.countDown()
            fireNext()
        }
      }
    }
    var k                = 0
    while (k < width) { fireNext(); k += 1 }
    remaining.await()
    val t                = failure.get()
    if (t != null) throw t // never publish numbers for a run where commands failed
    total.get()
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    slidingWindow(keys, concurrency)(async.get)(v => v.length.toLong)

  def setAll(keys: Array[String], value: String, concurrency: Int): Long = {
    val _ = slidingWindow(keys, concurrency)(k => async.set(k, value))(_ => 0L)
    keys.length.toLong
  }

  def mget(keys: Array[String]): Long =
    async.mget(keys*).get().asScala.iterator.filter(_.hasValue).map(_.getValue.length.toLong).sum

  def hgetall(key: String): Long = async.hgetall(key).get().size.toLong

  def close(): Unit = {
    conn.close()
    client.shutdown()
  }
}
