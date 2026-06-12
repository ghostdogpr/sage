package sage.examples.ox

import scala.concurrent.duration.*

import ox.Ox

import sage.*
import sage.examples.User
import sage.ox.*

/**
  * Commands across several families in direct style: every method returns its value directly inside an Ox scope (the `using Ox`).
  */
object CommandsExample {

  def run(client: SageClient)(using Ox): Unit = {
    // strings, numbers, and a conditional write with a TTL
    val _        = client.set("greeting", "hello")
    val greeting = client.get[String, String]("greeting")
    val _        = client.incrBy("counter", 10)
    val _        = client.set("session", "token", expiry = SetExpiry.In(1.minute), condition = SetCondition.IfNotExists)
    // hashes
    val _        = client.hSet("user:1", ("name", "Ada"), ("age", "36"))
    val profile  = client.hGetAll[String, String, String]("user:1")
    // lists: a blocking pop off a Dedicated Connection (data is pushed first, so it returns at once)
    val _        = client.rPush("queue", "a", "b", "c")
    val head     = client.blPop[String, String]("queue")(BlockTimeout.Forever)
    // sets and sorted sets
    val _        = client.sAdd("tags", "scala", "redis")
    val _        = client.zAdd("scores")(("ada", 36.0))
    // a custom-codec round-trip: User rides on the given ValueCodec from the shared module
    val _        = client.set("user:ada", User("Ada", 36))
    val ada      = client.get[String, User]("user:ada")
    // a raw Lua eval yields a Frame; decode it with the strict helpers
    val sum      = client.eval("return 1 + 1")
    println(s"greeting=$greeting profile=$profile head=$head ada=$ada sum=${sum.asLong}")
  }
}
