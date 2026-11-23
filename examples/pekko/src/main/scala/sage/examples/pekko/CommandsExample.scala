package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*

import sage.*
import sage.backend.*
import sage.examples.User

/**
  * Commands across several families, each returning a `scala.concurrent.Future`.
  */
object CommandsExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] =
    for {
      _        <- client.set("greeting", "hello")
      greeting <- client.get[String]("greeting")
      _        <- client.incrBy("counter", 10)
      _        <- client.set("session", "token", expiry = SetExpiry.In(1.minute), condition = SetCondition.IfNotExists)
      _        <- client.hSet("user:1", ("name", "Ada"), ("age", "36"))
      profile  <- client.hGetAll[String, String]("user:1")
      // lists: push data before the blocking pop so this example completes immediately
      _        <- client.rPush("queue", "a", "b", "c")
      head     <- client.blPop[String]("queue")(BlockTimeout.Forever)
      _        <- client.sAdd("tags", "scala", "redis")
      _        <- client.zAdd("scores")(("ada", 36.0))
      _        <- client.set("user:ada", User("Ada", 36))
      ada      <- client.get[User]("user:ada")
      // a raw Lua eval yields a Frame; decode it with the strict helpers
      sum      <- client.eval("return 1 + 1")
    } yield println(s"greeting=$greeting profile=$profile head=$head ada=$ada sum=${sum.asLong}")
}
