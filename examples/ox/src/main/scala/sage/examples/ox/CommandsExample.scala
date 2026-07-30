package sage.examples.ox

import scala.concurrent.duration.*

import ox.Ox

import sage.*
import sage.backend.*
import sage.examples.User

/**
  * Commands across several families in direct style: every method returns its value directly inside an Ox scope (the `using Ox`).
  */
object CommandsExample {

  def run(client: SageClient)(using Ox): Unit = {
    // strings, numbers, and a conditional write with a TTL
    client.set("greeting", "hello")
    val greeting = client.get[String]("greeting")
    client.incrBy("counter", 10)
    client.set("session", "token", expiry = SetExpiry.In(1.minute), condition = SetCondition.IfNotExists)
    // hashes
    client.hSet("user:1", ("name", "Ada"), ("age", "36"))
    val profile  = client.hGetAll[String, String]("user:1")
    // lists: a blocking pop off a Dedicated Connection (data is pushed first, so it returns at once)
    client.rPush("queue", "a", "b", "c")
    val head     = client.blPop[String]("queue")(BlockTimeout.Forever)
    // sets and sorted sets
    client.sAdd("tags", "scala", "redis")
    client.zAdd("scores")(("ada", 36.0))
    // a custom-codec round-trip: User rides on the given ValueCodec from the shared module
    client.set("user:ada", User("Ada", 36))
    val ada      = client.get[User]("user:ada")
    // a raw Lua eval yields a Frame; decode it with the strict helpers
    val sum      = client.eval("return 1 + 1")
    println(s"greeting=$greeting profile=$profile head=$head ada=$ada sum=${sum.asLong}")
  }
}
