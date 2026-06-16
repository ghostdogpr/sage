package sage.benchmarks

import kyo.*

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The Kyo cell's benchmark clients: sage only — no native Kyo competitor exists.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-kyo" => new SageKyoBench(host, port)
    case other      => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

private object Run {
  import AllowUnsafe.embrace.danger
  def apply[A](program: A < (Abort[Throwable] & Async)): A =
    KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
}

final class SageKyoBench(host: String, port: Int) extends BenchClient {

  private val client: SageClient =
    Run(SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))))

  def name: String = "sage-kyo"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit =
    Run(for {
      _ <- Kyo.foreachDiscard(0 until count)(i => client.set(s"$prefix:$i", value))
      _ <- Kyo.foreachDiscard(0 until fields)(i => client.hSet(hashKey, (s"f$i", value)))
    } yield ())

  def getAll(keys: Array[String], concurrency: Int): Long =
    Run(
      Async
        .foreach(Payloads.groups(keys, concurrency).toList, concurrency)(g =>
          Kyo.foreach(g.toList)(k => client.get[String](k)).map(_.toList)
        )
        .map(_.toList.flatten.flatten.map(_.length.toLong).sum)
    )

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Run(
      Async
        .foreachDiscard(Payloads.groups(keys, concurrency).toList, concurrency)(g => Kyo.foreachDiscard(g.toList)(k => client.set(k, value)))
        .map(_ => keys.length.toLong)
    )

  def mget(keys: Array[String]): Long =
    Run(client.mGet[String](keys.head, keys.tail*).map(_.flatten.map(_.length.toLong).sum))

  def hgetall(key: String): Long = Run(client.hGetAll[String, String](key).map(_.size.toLong))

  def close(): Unit = Run(client.close)
}
