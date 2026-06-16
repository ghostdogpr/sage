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

final class SageCeBench(host: String, port: Int) extends BenchClient {

  private val client: SageClient =
    SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))).unsafeRunSync()

  def name: String = "sage-ce"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val sets = (0 until count).toList.traverse_(i => client.set(s"$prefix:$i", value))
    val hash = (0 until fields).map(i => (s"f$i", value)).toList match {
      case h :: t => client.hSet(hashKey, h, t*).void
      case Nil    => IO.unit
    }
    (sets *> hash).unsafeRunSync()
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    Payloads
      .groups(keys, concurrency)
      .toList
      .parTraverse(_.toList.traverse(client.get[String]))
      .map(_.flatten.flatten.map(_.length.toLong).sum)
      .unsafeRunSync()

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Payloads
      .groups(keys, concurrency)
      .toList
      .parTraverse_(_.toList.traverse_(client.set(_, value)))
      .as(keys.length.toLong)
      .unsafeRunSync()

  def mget(keys: Array[String]): Long =
    client.mGet[String](keys.head, keys.tail*).map(_.flatten.map(_.length.toLong).sum).unsafeRunSync()

  def hgetall(key: String): Long = client.hGetAll[String, String](key).map(_.size.toLong).unsafeRunSync()

  def close(): Unit = client.close.unsafeRunSync()
}

final class Redis4catsBench(host: String, port: Int) extends BenchClient {

  private val (redis, release) = Redis[IO].utf8(s"redis://$host:$port").allocated.unsafeRunSync()

  def name: String = "redis4cats"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val sets = (0 until count).toList.traverse_(i => redis.set(s"$prefix:$i", value))
    val hash = (0 until fields).toList.traverse_(i => redis.hSet(hashKey, s"f$i", value))
    (sets *> hash).unsafeRunSync()
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    Payloads
      .groups(keys, concurrency)
      .toList
      .parTraverse(_.toList.traverse(redis.get))
      .map(_.flatten.flatten.map(_.length.toLong).sum)
      .unsafeRunSync()

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Payloads
      .groups(keys, concurrency)
      .toList
      .parTraverse_(_.toList.traverse_(redis.set(_, value)))
      .as(keys.length.toLong)
      .unsafeRunSync()

  def mget(keys: Array[String]): Long = redis.mGet(keys.toSet).map(_.values.map(_.length.toLong).sum).unsafeRunSync()

  def hgetall(key: String): Long = redis.hGetAll(key).map(_.size.toLong).unsafeRunSync()

  def close(): Unit = release.unsafeRunSync()
}
