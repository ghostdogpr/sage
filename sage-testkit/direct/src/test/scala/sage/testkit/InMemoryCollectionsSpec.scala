package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.ServerError
import sage.client.inmemory.Executor
import sage.protocol.Frame
import sage.ratelimit.RateLimiter

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

  test("raw commands reply as a server does, and unsupported commands and options fail with an error that names them") {
    val executor = new Executor
    Vector("SET hello Hello", "SET max 9223372036854775807", "SADD set m", "XADD x 1-1 a 1", "XGROUP CREATE x g $").foreach(raw(executor, _): Unit)

    def code(frame: Frame): String = frame match {
      case Frame.SimpleError(message) => message.takeWhile(_ != ' ')
      case other                      => other.toString
    }
    val codes                      = Vector(
      "SET hello v NX XX"                    -> "ERR",
      "INCR max"                             -> "ERR",
      "SETRANGE hello 9223372036854775807 x" -> "ERR",
      "ZSCAN hello 0"                        -> "WRONGTYPE",
      "XCLAIM x nog c 0 1-1"                 -> "NOGROUP",
      "XGROUP CREATECONSUMER x nog c"        -> "NOGROUP",
      "XINFO CONSUMERS missing g"            -> "ERR",
      "RENAME missing x"                     -> "ERR",
      "XGROUP CREATE x g $"                  -> "BUSYGROUP",
      s"EVALSHA ${"0" * 40} 0"               -> "NOSCRIPT"
    )
    codes.foreach((command, expected) => assertEquals(code(raw(executor, command)), expected, command))
    assertEquals(code(executor.execute("EVAL", Vector(RateLimiter.script, "0", "0", "1", "1", "v", "1").map(Bytes.utf8), 0L)), "SAGE")

    def bulk(text: String): Frame     = Frame.BulkString(Bytes.utf8(text))
    def array(items: Frame*): Frame   = Frame.Array(items.toVector)
    def error(message: String): Frame = Frame.SimpleError(s"ERR $message")
    val int                           = (n: Long) => Frame.Integer(n)
    // the SHA-1 digests of the scripts `return` and `return(1)`
    val (evaluated, loaded)           = ("63143b6f8007b98c53ca2149822777b3566f9241", "930269f31393d0be681588b6ab08dccee7d6bb67")
    val (first, second)               = (array(bulk("1-1"), array(bulk("a"), bulk("1"))), array(bulk("2-1"), array(bulk("b"), bulk("2"))))
    val replies                       = Vector(
      "GEOADD g 0 0 m"                                        -> error("unknown command 'GEOADD': the in-memory client does not support it"),
      "SCRIPT SHOW x"                                         -> error("unknown command 'SCRIPT SHOW': the in-memory client does not support it"),
      "SET hello v IFEQ Hello"                                -> error("the in-memory client does not support the IFEQ option of 'SET'"),
      "XINFO STREAM x FULL"                                   -> error("the in-memory client does not support the FULL option of 'XINFO STREAM'"),
      "EVAL return 0"                                         -> error("the in-memory client cannot run this Lua script; it only runs the scripts of Sage's locks and rate limiters"),
      "SCRIPT LOAD return(1)"                                 -> bulk(loaded),
      s"SCRIPT EXISTS $evaluated $loaded ${"0" * 40}"         -> array(int(1), int(1), int(0)),
      "GETRANGE hello 0 -10"                                  -> bulk("H"),
      "GETRANGE hello -8 -10"                                 -> bulk(""),
      "GETRANGE hello -4 -1"                                  -> bulk("ello"),
      "EXPIRE hello 10 GT"                                    -> int(0),
      "RPUSH one a"                                           -> int(1),
      "EXPIRE one 100"                                        -> int(1),
      "LMOVE one one LEFT RIGHT"                              -> bulk("a"),
      "TTL one"                                               -> int(100),
      "LPOS one a COUNT 9223372036854775807"                  -> array(int(0)),
      "LPOS one a RANK 4294967297"                            -> Frame.Null,
      "COPY one dst"                                          -> int(1),
      "TTL dst"                                               -> int(100),
      "LPUSH one b a"                                         -> int(3),
      "LREM one -1 a"                                         -> int(1),
      "LINDEX one -1"                                         -> bulk("b"),
      "KEYS [m-p]ne"                                          -> array(bulk("one")),
      "KEYS d[^x]t"                                           -> array(bulk("dst")),
      "KEYS d[t-]]t"                                          -> array(bulk("dst")),
      "HSET h f v g w"                                        -> int(2),
      "HSET h g w"                                            -> int(0),
      "HGETDEL h FIELDS 2 f f"                                -> array(bulk("v"), Frame.Null),
      "HDEL h g g"                                            -> int(1),
      "EXISTS h"                                              -> int(0),
      "ZADD zx 1 m 2 n"                                       -> int(2),
      "ZUNION 2 zx set WITHSCORES"                            -> array(array(bulk("m"), Frame.Double(2.0)), array(bulk("n"), Frame.Double(2.0))),
      "ZUNION 2 zx set AGGREGATE COUNT"                       -> array(bulk("n"), bulk("m")),
      "ZADD zx GT CH 0.5 m 5 n"                               -> int(1),
      "XADD xr 1-1 a 1"                                       -> bulk("1-1"),
      "XADD xr 2-1 b 2"                                       -> bulk("2-1"),
      "XRANGE xr (1-1 +"                                      -> array(second),
      "XRANGE xr - + COUNT 0"                                 -> Frame.Null,
      "XGROUP CREATE xr g 0"                                  -> Frame.SimpleString("OK"),
      // a stream named twice is read as the first read left it
      "XREADGROUP GROUP g c COUNT 1 STREAMS xr xr > >"        -> Frame.Map(Vector(bulk("xr") -> array(first), bulk("xr") -> array(second))),
      "XDEL xr 1-1"                                           -> int(1),
      // a history read reports an entry deleted from the stream with null fields and keeps it pending
      "XREADGROUP GROUP g c STREAMS xr 0"                     -> Frame.Map(Vector(bulk("xr") -> array(array(bulk("1-1"), Frame.Null), second))),
      "XCLAIM xr g d 0 2-1 IDLE 5 TIME 5 RETRYCOUNT 7 JUSTID" -> array(bulk("2-1")),
      "XPENDING xr g 2-1 2-1 1"                               -> array(array(bulk("2-1"), bulk("d"), int(0), int(7))),
      "XPENDING xr g - + 10 c"                                -> array(array(bulk("1-1"), bulk("c"), int(0), int(1))),
      // a `>` read gives an entry that is already pending to the reader with a delivery count of 1
      "XGROUP SETID xr g 0"                                   -> Frame.SimpleString("OK"),
      "XREADGROUP GROUP g e STREAMS xr >"                     -> Frame.Map(Vector(bulk("xr") -> array(second))),
      "XPENDING xr g 2-1 2-1 1"                               -> array(array(bulk("2-1"), bulk("e"), int(0), int(1))),
      "XTRIM xr MINID 3"                                      -> int(1),
      // XDEL raises the greatest deleted id that XSETID checks, and trimming leaves it as it was
      "XSETID xr 1-0"                                         -> error("The ID specified in XSETID is smaller than the target stream top item or the greatest deleted id"),
      "XSETID xr 2-0"                                         -> Frame.SimpleString("OK"),
      "XADD m 18446744073709551615-18446744073709551615 f v"  -> bulk("18446744073709551615-18446744073709551615"),
      "XADD m * f v"                                          -> error("The stream has exhausted the last possible ID, unable to add more items")
    )
    replies.foreach((command, reply) => assertEquals(raw(executor, command), reply, command))

    def blocking(command: String, now: Long = 0L): Either[Frame, ?] = {
      val words = command.split(" ").toVector
      executor.startBlocking(words.head, words.tail.map(Bytes.utf8), now).map(_.timeout)
    }
    assert(blocking("BLPOP k 1e300").isLeft)
    assert(blocking("XREAD BLOCK 10000000000000 STREAMS x 0").isRight)
    assert(blocking("BLPOP k 1", now = -1L).isRight)
    // a server adds the consumer before XREADGROUP blocks
    assert(blocking("XREADGROUP GROUP g joiner BLOCK 0 STREAMS x >").isRight)
    assertEquals(raw(executor, "XGROUP CREATECONSUMER x g joiner"), int(0))
    // a server reports an idle time of 0 when the clock went back, and only a `>` read without NOACK sets a consumer's active time
    raw(executor, "XADD x 2-1 a 1"): Unit
    raw(executor, "XREADGROUP GROUP g c NOACK STREAMS x >", now = 1000L): Unit
    def consumer(name: String): Frame                               =
      Frame.Map(Vector(bulk("name") -> bulk(name), bulk("pending") -> int(0), bulk("idle") -> int(0), bulk("inactive") -> int(-1)))
    assertEquals(raw(executor, "XINFO CONSUMERS x g"), array(consumer("c"), consumer("joiner")))
    assertEquals(raw(executor, "SET early v PX 999", now = -1000L), Frame.SimpleString("OK"))
    assertEquals(raw(executor, "PTTL early", now = -1000L), Frame.Integer(999))
  }
}
