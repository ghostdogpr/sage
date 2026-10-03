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

  def buildTopology(host: String, port: Int, name: String, topology: String): BenchClient = {
    val endpoint = Endpoint(host, port)
    (name, topology) match {
      case ("sage-zio", "standalone")     => new SageZioBench(Topology.Standalone(endpoint))
      case ("sage-zio", "cluster")        => new SageZioBench(Topology.Cluster(Vector(endpoint)))
      case ("sage-zio", "master-replica") => new SageZioBench(Topology.MasterReplica(Vector(endpoint)))
      case other                          => throw new IllegalArgumentException(s"unknown client and topology: $other")
    }
  }
}

private object Run {
  val runtime: Runtime[Any]                  = Runtime.default
  def apply[A](z: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe(implicit u => runtime.unsafe.run(z).getOrThrowFiberFailure())
}

final class SageZioBench(topology: Topology) extends SageBench[IO[SageException, *]] {

  protected val client: SageClient = Run(SageClient.connect(SageConfig(topology = topology)))

  protected def run[A](effect: IO[SageException, A]): Unit = Run(effect): Unit

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => IO[SageException, A]): IO[SageException, Unit] =
    ZIO.foreachParDiscard(work.lanes)(ZIO.foreachDiscard(_)(perKey))
}

final class ZioRedisBench(host: String, port: Int) extends BenchClient {

  // The benchmark uses String keys and values. Use a raw UTF-8 codec for a fair comparison with Sage, redis4cats, and Lettuce. The zio-redis
  // Protobuf default would add serialization work for every element.
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

  def getAll(work: Payloads.Workload): Unit =
    Run(ZIO.foreachParDiscard(work.lanes)(g => ZIO.foreachDiscard(g)(k => redis.get(k).returning[String])))

  def setAll(work: Payloads.Workload, value: String): Unit =
    Run(ZIO.foreachParDiscard(work.lanes)(g => ZIO.foreachDiscard(g)(k => redis.set(k, value))))

  def mget(): Unit = Run(redis.mGet(Payloads.Keys.first, Payloads.Keys.rest*).returning[String].unit)

  def hgetall(): Unit = Run(redis.hGetAll(Payloads.HashKey).returning[String, String].unit)

  def close(): Unit = Run(scope.close(Exit.unit))
}
