package sage.benchmarks

import java.util.concurrent.{CompletableFuture, CountDownLatch, Executors}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.Failure

import _root_.ox.{fork, supervised, Ox}
import io.lettuce.core.RedisClient
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
final class SageOxBench(host: String, port: Int) extends SageBench[[A] =>> Ox ?=> A] {

  private val opened   = new CompletableFuture[SageClient]
  private val shutdown = new CountDownLatch(1)

  private val holder               = Thread.ofVirtual().start { () =>
    try
      supervised {
        opened.complete(SageClient.scoped(SageConfig(topology = Topology.Standalone(Endpoint(host, port))))): Unit
        shutdown.await()
      }
    catch { case t: Throwable => opened.completeExceptionally(t): Unit }
  }
  protected val client: SageClient = opened.join()

  protected def run[A](effect: Ox ?=> A): Unit = supervised(effect): Unit

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => Ox ?=> A): Ox ?=> Unit =
    work.lanes.map(g => fork(g.foreach(perKey(_): A))).foreach(_.join())

  override def close(): Unit = {
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

  def getAll(work: Payloads.Workload): Unit =
    SlidingWindow(work)((k, done) => async.get(k).whenComplete((_, t) => done(t)): Unit).run()

  def setAll(work: Payloads.Workload, value: String): Unit =
    SlidingWindow(work)((k, done) => async.set(k, value).whenComplete((_, t) => done(t)): Unit).run()

  def mget(): Unit = async.mget(Payloads.Keys.all*).get(): Unit

  def hgetall(): Unit = async.hgetall(Payloads.HashKey).get(): Unit

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

  private def await[A](f: Future[A]): A = Await.result(f, 5.minutes)

  def getAll(work: Payloads.Workload): Unit =
    SlidingWindow(work)((k, done) => client.get[String](k).onComplete { case Failure(t) => done(t); case _ => done(null) }).run()

  def setAll(work: Payloads.Workload, value: String): Unit =
    SlidingWindow(work)((k, done) => client.set(k, value).onComplete { case Failure(t) => done(t); case _ => done(null) }).run()

  def mget(): Unit = await(client.mget[String](Payloads.Keys.all*)): Unit

  def hgetall(): Unit = await(client.hgetall[String](Payloads.HashKey)): Unit

  def close(): Unit = {
    client.stop()
    Await.result(system.terminate(), 30.seconds): Unit
  }
}

/**
  * Keeps up to `concurrency` requests in flight and starts the next one whenever one completes, so no request waits for the slowest member
  * of a fixed batch. `submit` starts the request for a key and calls `done(failure)` once, with a null `failure` on success.
  */
final private class SlidingWindow(work: Payloads.Workload)(submit: (String, Throwable => Unit) => Unit) {
  private val nextIndex = new AtomicInteger(0)
  private val remaining = new CountDownLatch(Payloads.Keys.all.length)
  private val failure   = new AtomicReference[Throwable]()

  private val done: Throwable => Unit = t => {
    if (t != null) failure.compareAndSet(null, t): Unit
    remaining.countDown()
    fireNext()
  }

  private def fireNext(): Unit = {
    val i = nextIndex.getAndIncrement()
    if (i < Payloads.Keys.all.length) {
      try submit(Payloads.Keys.all(i), done)
      catch { case t: Throwable => done(t) }
    }
  }

  def run(): Unit = {
    var k = 0
    while (k < work.concurrency) {
      fireNext()
      k += 1
    }
    remaining.await()
    val t = failure.get()
    if (t != null) throw t // never publish numbers for a run where commands failed
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

  // one lane per group on a virtual thread, each with its own borrowed connection running blocking commands sequentially
  private def lanes(work: Payloads.Workload)(run: (redis.clients.jedis.Jedis, String) => Any): Unit =
    work.lanes
      .map(g => executor.submit[Unit](() => borrow(j => g.foreach(run(j, _)))))
      .foreach(_.get())

  def getAll(work: Payloads.Workload): Unit = lanes(work)(_.get(_))

  def setAll(work: Payloads.Workload, value: String): Unit = lanes(work)(_.set(_, value))

  def mget(): Unit = borrow(_.mget(Payloads.Keys.all*)): Unit

  def hgetall(): Unit = borrow(_.hgetAll(Payloads.HashKey)): Unit

  private inline def borrow[A](inline f: redis.clients.jedis.Jedis => A): A = {
    val j = pool.getResource()
    try f(j)
    finally j.close()
  }

  def close(): Unit = {
    executor.shutdown()
    pool.close()
  }
}
