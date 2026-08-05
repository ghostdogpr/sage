package sage.benchmarks

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

import _root_.ox.{fork, supervised}
import io.lettuce.core.{RedisClient, RedisFuture}
import org.apache.pekko.actor.ActorSystem
import redis.clients.jedis.{DefaultJedisClientConfig, HostAndPort, JedisPool, RedisProtocol}

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * Clients included in the Ox benchmark: Sage in direct style and Lettuce using its asynchronous auto-pipelined API.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-ox"   => new SageOxBench(host, port)
    case "lettuce"   => new LettuceBench(host, port)
    case "rediscala" => new RediscalaBench(host, port)
    case "jedis"     => new JedisBench(host, port)
    case other       => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

/**
  * Sage's Ox API requires an `Ox` scope. A holder fiber keeps the client's scope open for the benchmark lifetime, while each benchmark
  * operation runs in a short-lived supervised scope and shares the same connection.
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
      client.set(s"$prefix:$i", value)
    }
    (0 until fields).foreach { i =>
      client.hSet(hashKey, (s"f$i", value))
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
          client.set(k, value)
          n + 1
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
  * Lettuce using its asynchronous auto-pipelined API. Up to `concurrency` commands remain in flight, allowing the shared connection to
  * combine them into fewer socket writes.
  */
final class LettuceBench(host: String, port: Int) extends BenchClient {

  private val client = RedisClient.create(s"redis://$host:$port")
  private val conn   = client.connect()
  private val async  = conn.async()

  def name: String = "lettuce"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val writes = (0 until count).map(i => async.set(s"$prefix:$i", value)) ++ (0 until fields).map(i => async.hset(hashKey, s"f$i", value))
    writes.foreach { f =>
      f.get()
    }
  }

  // Keep up to concurrency futures in flight, starting the next request whenever one completes. This avoids making all requests wait for
  // the slowest member of a fixed batch. Completion callbacks run on Lettuce's event loop.
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
        try
          submit(keys(i)).whenComplete { (v, t) =>
            if (t != null) { failure.compareAndSet(null, t): Unit }
            else if (v != null) { total.addAndGet(score(v)): Unit }
            remaining.countDown()
            fireNext()
          }: Unit
        catch {
          case t: Throwable =>
            failure.compareAndSet(null, t)
            remaining.countDown()
            fireNext()
        }
      }
    }
    var k                = 0
    while (k < width) {
      fireNext()
      k += 1
    }
    remaining.await()
    val t                = failure.get()
    if (t != null) throw t // never publish numbers for a run where commands failed
    total.get()
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    slidingWindow(keys, concurrency)(async.get)(v => v.length.toLong)

  def setAll(keys: Array[String], value: String, concurrency: Int): Long = {
    slidingWindow(keys, concurrency)(k => async.set(k, value))(_ => 0L)
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

/**
  * Rediscala (Apache Pekko actor + Future based, auto-pipelined). Driven like Lettuce: a sliding window keeps exactly `concurrency` futures
  * in flight so the shared connection coalesces them, with completions running on the Pekko dispatcher.
  */
final class RediscalaBench(host: String, port: Int) extends BenchClient {

  private given system: ActorSystem = ActorSystem("rediscala-bench")
  import system.dispatcher
  private val client                = redis.RedisClient(host, port)

  def name: String = "rediscala"

  private def await[A](f: Future[A]): A = Await.result(f, 5.minutes)

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val writes = (0 until count).map(i => client.set(s"$prefix:$i", value)) ++ (0 until fields).map(i => client.hset(hashKey, s"f$i", value))
    writes.foreach { f =>
      await(f)
    }
  }

  // match LettuceBench.slidingWindow by keeping up to concurrency futures in flight and starting the next request after each completion.
  private def slidingWindow[T](keys: Array[String], concurrency: Int)(submit: String => Future[T])(score: T => Long): Long = {
    val n                = keys.length
    val width            = math.max(1, math.min(concurrency, n))
    val total            = new AtomicLong(0L)
    val nextIndex        = new AtomicInteger(0)
    val remaining        = new CountDownLatch(n)
    val failure          = new AtomicReference[Throwable]()
    def fireNext(): Unit = {
      val i = nextIndex.getAndIncrement()
      if (i < n)
        submit(keys(i)).onComplete { result =>
          result match {
            case Success(v) => total.addAndGet(score(v))
            case Failure(t) => failure.compareAndSet(null, t)
          }
          remaining.countDown()
          fireNext()
        }
    }
    var k                = 0
    while (k < width) {
      fireNext()
      k += 1
    }
    remaining.await()
    val t                = failure.get()
    if (t != null) throw t // never publish numbers for a run where commands failed
    total.get()
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    slidingWindow(keys, concurrency)(k => client.get[String](k))(_.fold(0L)(_.length.toLong))

  def setAll(keys: Array[String], value: String, concurrency: Int): Long = {
    slidingWindow(keys, concurrency)(k => client.set(k, value))(_ => 0L)
    keys.length.toLong
  }

  def mget(keys: Array[String]): Long = await(client.mget[String](keys*)).flatten.map(_.length.toLong).sum

  def hgetall(key: String): Long = await(client.hgetall[String](key)).size.toLong

  def close(): Unit = {
    client.stop()
    Await.result(system.terminate(), 30.seconds): Unit
  }
}

/**
  * Jedis is synchronous and blocking. The benchmark runs `concurrency` lanes on virtual threads, with each lane borrowing its own pooled
  * connection. Jedis does not pipeline these commands automatically. RESP3 is enabled to match the other clients.
  */
final class JedisBench(host: String, port: Int) extends BenchClient {

  private val config   = DefaultJedisClientConfig.builder().protocol(RedisProtocol.RESP3).build()
  private val poolCfg  = {
    val c = new org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Jedis]()
    c.setMaxTotal(512)
    c.setMaxIdle(512)
    c
  }
  private val pool     = new JedisPool(poolCfg, new HostAndPort(host, port), config)
  private val executor = Executors.newVirtualThreadPerTaskExecutor()

  def name: String = "jedis"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val j = pool.getResource()
    try {
      val p = j.pipelined()
      (0 until count).foreach { i =>
        p.set(s"$prefix:$i", value)
      }
      (0 until fields).foreach { i =>
        p.hset(hashKey, s"f$i", value)
      }
      p.sync()
    } finally j.close()
  }

  // one lane per group on a virtual thread, each with its own borrowed connection running blocking commands sequentially
  private def lanes(keys: Array[String], concurrency: Int)(run: (redis.clients.jedis.Jedis, Array[String]) => Long): Long =
    Payloads
      .groups(keys, concurrency)
      .map(g =>
        executor.submit[Long] { () =>
          val j = pool.getResource()
          try run(j, g)
          finally j.close()
        }
      )
      .map(_.get())
      .sum

  def getAll(keys: Array[String], concurrency: Int): Long =
    lanes(keys, concurrency)((j, g) => g.foldLeft(0L)((t, k) => t + Option(j.get(k)).fold(0L)(_.length.toLong)))

  def setAll(keys: Array[String], value: String, concurrency: Int): Long = {
    lanes(keys, concurrency) { (j, g) =>
      g.foreach { k =>
        j.set(k, value)
      }
      g.length.toLong
    }
    keys.length.toLong
  }

  def mget(keys: Array[String]): Long = {
    val j = pool.getResource()
    try j.mget(keys*).asScala.iterator.filter(_ != null).map(_.length.toLong).sum
    finally j.close()
  }

  def hgetall(key: String): Long = {
    val j = pool.getResource()
    try j.hgetAll(key).size.toLong
    finally j.close()
  }

  def close(): Unit = {
    executor.shutdown()
    pool.close()
  }
}
