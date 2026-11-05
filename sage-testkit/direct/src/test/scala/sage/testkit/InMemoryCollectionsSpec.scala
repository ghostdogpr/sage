package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.ServerError
import sage.client.inmemory.Executor
import sage.protocol.Frame

class InMemoryCollectionsSpec extends InMemorySuite {

  private def wrongType(body: => Any): Unit = assertEquals(intercept[ServerError](body).code, "WRONGTYPE")

  clients.test("every collection command checks the type of the key") { client =>
    client.set("s", "v"): Unit
    wrongType(client.hGet[String, String]("s", "f"))
    wrongType(client.lPush("s", "x"))
    wrongType(client.sAdd("s", "x"))
    wrongType(client.zAdd("s")("x" -> 1.0))
    wrongType(client.get[String] { client.rPush("list", "x"): Unit; "list" })
    assertEquals(client.typeOf("list"), Some(RedisType.List))
    client.zAdd("zs")("x" -> 1.0): Unit
    assertEquals(client.typeOf("zs"), Some(RedisType.ZSet))
  }

  clocked.test("collections keep the key's expiry when they change") { (client, clock) =>
    client.rPush("l", "a"): Unit
    client.expire("l", 10.seconds): Unit
    client.rPush("l", "b"): Unit
    assertEquals(client.ttl("l"), Ttl.Expires(10.seconds))
    clock.advance(10.seconds)
    assertEquals(client.lLen("l"), 0L)
  }

  test("raw commands reply as a server does") {
    val executor = new Executor
    Vector("SET hello Hello", "SET max 9223372036854775807", "SADD set m").foreach(raw(executor, _): Unit)

    def code(frame: Frame): String = frame match {
      case Frame.SimpleError(message) => message.takeWhile(_ != ' ')
      case other                      => other.toString
    }
    val codes                      = Vector(
      "SET hello v NX XX" -> "ERR",
      "INCR max"          -> "ERR",
      "ZSCAN hello 0"     -> "WRONGTYPE",
      "RENAME missing x"  -> "ERR"
    )
    codes.foreach((command, expected) => assertEquals(code(raw(executor, command)), expected, command))

    def bulk(text: String): Frame   = Frame.BulkString(Bytes.utf8(text))
    def array(items: Frame*): Frame = Frame.Array(items.toVector)
    val int                         = (n: Long) => Frame.Integer(n)
    val replies                     = Vector(
      "RPUSH one a"                 -> int(1),
      "EXPIRE one 100"              -> int(1),
      "LMOVE one one LEFT RIGHT"    -> bulk("a"),
      "TTL one"                     -> int(100),
      "LPUSH one b a"               -> int(3),
      "LREM one -1 a"               -> int(1),
      "LINDEX one -1"               -> bulk("b"),
      "RENAME hello dst"            -> Frame.SimpleString("OK"),
      "KEYS [m-p]ne"                -> array(bulk("one")),
      "KEYS d[^x]t"                 -> array(bulk("dst")),
      "KEYS d[t-]]t"                -> array(bulk("dst")),
      "HSET h f v g w"              -> int(2),
      "HSET h g w"                  -> int(0),
      "HDEL h f g g"                -> int(2),
      "EXISTS h"                    -> int(0),
      "SADD other m n"              -> int(2),
      "SINTER set other"            -> Frame.Set(Vector(bulk("m"))),
      "SDIFF other set"             -> Frame.Set(Vector(bulk("n"))),
      "ZADD zx 1 m 2 n 3 o"         -> int(3),
      "ZADD zx GT CH 0.5 m 5 n"     -> int(1),
      "ZREMRANGEBYSCORE zx (1 3"    -> int(1),
      "ZREMRANGEBYRANK zx 0 0"      -> int(1),
      "BZPOPMAX zx 1"               -> array(bulk("zx"), bulk("n"), Frame.Double(5.0)),
      "ZADD zp 1 a 2 b 3 c"         -> int(3),
      "ZPOPMIN zp"                  -> array(bulk("a"), Frame.Double(1.0)),
      "ZPOPMAX zp 2"                -> array(array(bulk("c"), Frame.Double(3.0)), array(bulk("b"), Frame.Double(2.0))),
      "SUNION set other"            -> Frame.Set(Vector(bulk("m"), bulk("n"))),
      "STRLEN dst"                  -> int(5),
      "BLMOVE one two RIGHT LEFT 1" -> bulk("b")
    )
    replies.foreach((command, reply) => assertEquals(raw(executor, command), reply, command))

    assert(executor.startBlocking("BLPOP", Vector("k", "10000000000").map(Bytes.utf8)).isRight)
    assertEquals(raw(executor, "SET early v PX 999", now = -1000L), Frame.SimpleString("OK"))
    assertEquals(raw(executor, "PTTL early", now = -1000L), Frame.Integer(999))
  }
}
