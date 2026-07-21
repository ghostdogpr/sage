package sage.integration.commands

import kyo.compat.*

import sage.commands.JsonPath
import sage.integration.{Images, ServerSuite}

/**
  * JSON commands present only on Redis (RedisJSON), not on Valkey Bundle 9.1.0: `JSON.MERGE` (RFC 7386). Mirrors the Redis-only extras suites
  * of other families (ADR-0026); the shared [[JsonSuite]] stays backend-symmetric.
  */
class RedisJsonExtrasSuite extends ServerSuite(Images.redis) {

  test("JSON.MERGE updates existing and creates new members") {
    withClient { client =>
      for {
        _ <- client.jsonSet("merge", JsonPath.root, """{"a":1,"b":2}""")
        _ <- client.jsonMerge("merge", JsonPath.root, """{"b":20,"c":3}""")
        a <- client.jsonGet[String]("merge", JsonPath("$.a"))
        b <- client.jsonGet[String]("merge", JsonPath("$.b"))
        c <- client.jsonGet[String]("merge", JsonPath("$.c"))
      } yield {
        assert(a.exists(_.contains("1")))
        assert(b.exists(_.contains("20")))
        assert(c.exists(_.contains("3")))
      }
    }
  }
}
