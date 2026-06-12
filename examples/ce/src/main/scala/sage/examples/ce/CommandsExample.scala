package sage.examples.ce

import scala.concurrent.duration.*

import cats.effect.IO

import sage.*
import sage.ce.*
import sage.examples.User

/**
  * Commands across several families, each returning a cats-effect `IO`.
  */
object CommandsExample {

  def run(client: SageClient): IO[Unit] =
    for {
      // strings, numbers, and a conditional write with a TTL
      _        <- client.set("greeting", "hello")
      greeting <- client.get[String, String]("greeting")
      _        <- client.incrBy("counter", 10)
      _        <- client.set("session", "token", expiry = SetExpiry.In(1.minute), condition = SetCondition.IfNotExists)
      // hashes
      _        <- client.hSet("user:1", ("name", "Ada"), ("age", "36"))
      profile  <- client.hGetAll[String, String, String]("user:1")
      // lists: a blocking pop off a Dedicated Connection (data is pushed first, so it returns at once)
      _        <- client.rPush("queue", "a", "b", "c")
      head     <- client.blPop[String, String]("queue")(BlockTimeout.Forever)
      // sets and sorted sets
      _        <- client.sAdd("tags", "scala", "redis")
      _        <- client.zAdd("scores")(("ada", 36.0))
      // a custom-codec round-trip: User rides on the given ValueCodec from the shared module
      _        <- client.set("user:ada", User("Ada", 36))
      ada      <- client.get[String, User]("user:ada")
      // a raw Lua eval yields a Frame; decode it with the strict helpers
      sum      <- client.eval("return 1 + 1")
      _        <- IO.println(s"greeting=$greeting profile=$profile head=$head ada=$ada sum=${sum.asLong}")
    } yield ()
}
