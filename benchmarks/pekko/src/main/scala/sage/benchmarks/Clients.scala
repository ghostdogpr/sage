package sage.benchmarks

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The Pekko cell's benchmark clients: sage only. There is no Pekko-based Redis client to use as a competitor baseline, so this cell measures
  * the sage Future surface on its own, the same way the Kyo cell does.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-pekko" => new SagePekkoBench(host, port)
    case other        => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

final class SagePekkoBench(host: String, port: Int) extends BenchClient {

  private given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sage-bench")
  private given ExecutionContext             = system.executionContext

  private val client: SageClient =
    Await.result(SageClient.connectUnmanaged(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))), 30.seconds)

  // bounds in-flight commands by running each item of a group sequentially while groups run in parallel
  private def seqTraverse[A, B](as: List[A])(f: A => Future[B]): Future[List[B]] =
    as.foldLeft(Future.successful(List.empty[B]))((accF, a) => accF.flatMap(acc => f(a).map(_ :: acc))).map(_.reverse)

  private def seqRun[A](as: List[A])(f: A => Future[Any]): Future[Unit] =
    as.foldLeft(Future.unit)((accF, a) => accF.flatMap(_ => f(a).map(_ => ())))

  def name: String = "sage-pekko"

  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit = {
    val sets = seqRun((0 until count).toList)(i => client.set(s"$prefix:$i", value))
    val hash = (0 until fields).map(i => (s"f$i", value)).toList match {
      case h :: t => client.hSet(hashKey, h, t*).map(_ => ())
      case Nil    => Future.unit
    }
    val _    = Await.result(sets.flatMap(_ => hash), 120.seconds)
  }

  def getAll(keys: Array[String], concurrency: Int): Long =
    Await.result(
      Future
        .traverse(Payloads.groups(keys, concurrency).toList)(g => seqTraverse(g.toList)(client.get[String]))
        .map(_.flatten.flatten.map(_.length.toLong).sum),
      120.seconds
    )

  def setAll(keys: Array[String], value: String, concurrency: Int): Long =
    Await.result(
      Future
        .traverse(Payloads.groups(keys, concurrency).toList)(g => seqRun(g.toList)(client.set(_, value)))
        .map(_ => keys.length.toLong),
      120.seconds
    )

  def mget(keys: Array[String]): Long =
    Await.result(client.mGet[String](keys.head, keys.tail*).map(_.flatten.map(_.length.toLong).sum), 120.seconds)

  def hgetall(key: String): Long =
    Await.result(client.hGetAll[String, String](key).map(_.size.toLong), 120.seconds)

  def close(): Unit = {
    val _ = Await.result(client.close, 30.seconds)
    system.terminate()
    val _ = Await.ready(system.whenTerminated, 10.seconds)
  }
}
