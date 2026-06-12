package sage.examples.kyo

import java.util.concurrent.TimeUnit

import kyo.*

import sage.*
import sage.examples.User
import sage.kyo.*

/**
  * Commands across several families, each returning a Kyo computation in the `Abort[Throwable] & Async` effect set.
  */
object CommandsExample {

  // SET's expiry takes a scala FiniteDuration (Kyo's own Duration is used for kyo APIs like Async.sleep)
  private val oneMinute: scala.concurrent.duration.FiniteDuration =
    scala.concurrent.duration.FiniteDuration(1L, TimeUnit.MINUTES)

  def run(client: SageClient): Unit < (Abort[Throwable] & Async) =
    for {
      // strings, numbers, and a conditional write with a TTL
      _        <- client.set("greeting", "hello")
      greeting <- client.get[String, String]("greeting")
      _        <- client.incrBy("counter", 10)
      _        <- client.set("session", "token", expiry = SetExpiry.In(oneMinute), condition = SetCondition.IfNotExists)
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
      _        <- Console.printLine(s"greeting=$greeting profile=$profile head=$head ada=$ada sum=${sum.asLong}")
    } yield ()
}
