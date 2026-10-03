package sage.benchmarks

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log.NoOp.given

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The Cats Effect cell's benchmark clients: sage (native) and redis4cats (which wraps Lettuce).
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-ce"    => new SageCeBench(host, port)
    case "redis4cats" => new Redis4catsBench(host, port)
    case other        => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

final class SageCeBench(host: String, port: Int) extends SageBench[IO] {

  protected val client: SageClient =
    SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))).unsafeRunSync()

  protected def run[A](effect: IO[A]): Unit = effect.unsafeRunSync(): Unit

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => IO[A]): IO[Unit] = work.lanes.parTraverse_(_.traverse_(perKey))
}

final class Redis4catsBench(host: String, port: Int) extends BenchClient {

  private val (redis, release) = Redis[IO].utf8(s"redis://$host:$port").allocated.unsafeRunSync()

  def getAll(work: Payloads.Workload): Unit =
    work.lanes.parTraverse_(_.traverse_(redis.get)).unsafeRunSync()

  def setAll(work: Payloads.Workload, value: String): Unit =
    work.lanes.parTraverse_(_.traverse_(redis.set(_, value))).unsafeRunSync()

  def mget(): Unit = redis.mGet(Payloads.Keys.set).void.unsafeRunSync()

  def hgetall(): Unit = redis.hGetAll(Payloads.HashKey).void.unsafeRunSync()

  def close(): Unit = release.unsafeRunSync()
}
