package sage.benchmarks

import java.nio.charset.StandardCharsets.UTF_8

import zio.*
import zio.redis.*
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, DecodeError}
import zio.stream.ZPipeline

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The ZIO cell's benchmark clients: sage (native) and zio-redis (native).
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-zio"  => new SageZioBench(Topology.Standalone(Endpoint(host, port)))
    case "zio-redis" => new ZioRedisBench(host, port)
    case other       => throw new IllegalArgumentException(s"unknown client: $other")
  }

  def buildTopology(host: String, port: Int, topology: String): BenchClient = {
    val endpoint = Endpoint(host, port)
    topology match {
      case "standalone"     => new SageZioBench(Topology.Standalone(endpoint))
      case "cluster"        => new SageZioBench(Topology.Cluster(Vector(endpoint)))
      case "master-replica" => new SageZioBench(Topology.MasterReplica(Vector(endpoint)))
      case other            => throw new IllegalArgumentException(s"unknown topology: $other")
    }
  }
}

private object Run {
  val runtime: Runtime[Any]                  = Runtime.default
  def apply[A](z: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe(implicit u => runtime.unsafe.run(z).getOrThrowFiberFailure())
}

final class SageZioBench(topology: Topology) extends BenchClient {

  private val client: SageClient =
    Run(SageClient.connect(SageConfig(topology = topology)))

  def name: String = "sage-zio"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit =
    Run(
      ZIO.foreachDiscard(0 until count)(i => client.set(s"$prefix:$i", value)) *>
        ZIO.foreachDiscard(0 until fields)(i => client.hSet(hashKey, (s"f$i", value)))
    )

  def getAll(keys: Array[String], concurrency: Int): Long =
    Run(
      ZIO
        .foreachPar(Payloads.groups(keys, concurrency).toList)(g => ZIO.foreach(g.toList)(k => client.get[String](k)))
        .map(_.flatten.flatten.map(_.length.toLong).sum)
    )

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Run(
      ZIO
        .foreachParDiscard(Payloads.groups(keys, concurrency).toList)(g => ZIO.foreachDiscard(g.toList)(k => client.set(k, value)))
        .as(keys.length.toLong)
    )

  def mget(keys: Array[String]): Long =
    Run(client.mGet[String](keys.head, keys.tail*).map(_.flatten.map(_.length.toLong).sum))

  def hgetall(key: String): Long = Run(client.hGetAll[String, String](key).map(_.size.toLong))

  def close(): Unit = Run(client.close)
}

final class ZioRedisBench(host: String, port: Int) extends BenchClient {

  // the bench only uses String keys/values; use a raw UTF-8 codec to match what sage/redis4cats/lettuce do, rather than zio-redis's
  // protobuf default which would charge it per-element serialization the others never pay
  private val utf8: BinaryCodec[String] = new BinaryCodec[String] {
    def encode(s: String): Chunk[Byte]                           = Chunk.fromArray(s.getBytes(UTF_8))
    def decode(bytes: Chunk[Byte]): Either[DecodeError, String]  = Right(new String(bytes.toArray, UTF_8))
    def streamEncoder: ZPipeline[Any, Nothing, String, Byte]     = ZPipeline.mapChunks(_.flatMap(s => Chunk.fromArray(s.getBytes(UTF_8))))
    def streamDecoder: ZPipeline[Any, DecodeError, Byte, String] = ZPipeline.mapChunks(b => Chunk.single(new String(b.toArray, UTF_8)))
  }
  private object Utf8CodecSupplier extends CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = utf8.asInstanceOf[BinaryCodec[A]]
  }

  private val scope: Scope.Closeable = Run(Scope.make)
  private val redis: Redis           =
    Run(
      scope.extend(
        ZLayer
          .make[Redis](ZLayer.succeed(RedisConfig(host, port)), ZLayer.succeed[CodecSupplier](Utf8CodecSupplier), Redis.singleNode)
          .build
          .map(_.get[Redis])
      )
    )

  def name: String = "zio-redis"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit =
    Run(
      ZIO.foreachDiscard(0 until count)(i => redis.set(s"$prefix:$i", value)) *>
        ZIO.foreachDiscard(0 until fields)(i => redis.hSet(hashKey, (s"f$i", value)))
    )

  def getAll(keys: Array[String], concurrency: Int): Long =
    Run(
      ZIO
        .foreachPar(Payloads.groups(keys, concurrency).toList)(g => ZIO.foreach(g.toList)(k => redis.get(k).returning[String]))
        .map(_.flatten.flatten.map(_.length.toLong).sum)
    )

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Run(
      ZIO
        .foreachParDiscard(Payloads.groups(keys, concurrency).toList)(g => ZIO.foreachDiscard(g.toList)(k => redis.set(k, value)))
        .as(keys.length.toLong)
    )

  def mget(keys: Array[String]): Long =
    Run(redis.mGet(keys.head, keys.tail*).returning[String].map(_.flatten.map(_.length.toLong).sum))

  def hgetall(key: String): Long = Run(redis.hGetAll(key).returning[String, String].map(_.size.toLong))

  def close(): Unit = Run(scope.close(Exit.unit))
}
