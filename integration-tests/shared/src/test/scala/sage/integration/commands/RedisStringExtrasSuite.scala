package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.{Images, ServerSuite}

/**
  * DIGEST/DELEX/MSETEX/INCREX are Redis-only string commands (absent in Valkey 8.1), so they have no cross-server counterpart.
  */
class RedisStringExtrasSuite extends ServerSuite(Images.redis) {

  test("DIGEST returns a stable hex digest, None for a missing key") {
    withClient { client =>
      for {
        _    <- client.set("sx-digest", "hello")
        d1   <- client.digest("sx-digest")
        d2   <- client.digest("sx-digest")
        none <- client.digest("sx-digest-missing")
      } yield {
        assert(d1.exists(_.nonEmpty))
        assertEquals(d1, d2)
        assertEquals(none, None)
      }
    }
  }

  test("DELEX deletes only when the value or digest condition matches") {
    withClient { client =>
      for {
        _       <- client.set("sx-delex", "v1")
        noMatch <- client.delex("sx-delex", DelexCondition.IfEq("other"))
        present <- client.exists("sx-delex")
        digest  <- client.digest("sx-delex")
        notNe   <- client.delex[String, String]("sx-delex", DelexCondition.IfDigestNe(digest.get))
        matched <- client.delex("sx-delex", DelexCondition.IfEq("v1"))
        gone    <- client.exists("sx-delex")
      } yield {
        assertEquals(noMatch, false)
        assertEquals(present, 1L)
        assertEquals(notNe, false)
        assertEquals(matched, true)
        assertEquals(gone, 0L)
      }
    }
  }

  test("MSETEX sets multiple keys with a shared TTL and respects NX") {
    withClient { client =>
      for {
        set     <- client.msetEx(expiry = SetExpiry.In(100.seconds))(("sx-ms-a", "1"), ("sx-ms-b", "2"))
        a       <- client.get[String, String]("sx-ms-a")
        ttl     <- client.ttl("sx-ms-a")
        blocked <- client.msetEx(condition = SetCondition.IfNotExists)(("sx-ms-a", "9"))
        aStill  <- client.get[String, String]("sx-ms-a")
      } yield {
        assertEquals(set, true)
        assertEquals(a, Some("1"))
        assert(ttl match {
          case Ttl.Expires(d) => d > Duration.Zero
          case _              => false
        })
        assertEquals(blocked, false)
        assertEquals(aStill, Some("1"))
      }
    }
  }

  test("INCREX increments with expiry, saturating bounds, and rejects when out of range") {
    withClient { client =>
      for {
        first  <- client.increxBy("sx-incr", 5L, expiry = IncrExpiry.In(100.seconds))
        ttl    <- client.ttl("sx-incr")
        capped <- client.increxBy("sx-incr", 100L, saturate = true, upperBound = Some(10L))
        reject <- client.increxBy("sx-incr", 100L, upperBound = Some(10L))
        viaFlt <- client.increxByFloat("sx-incr-f", 1.5)
      } yield {
        assertEquals(first, IncrExResult(5L, 5L))
        assert(ttl match {
          case Ttl.Expires(d) => d > Duration.Zero
          case _              => false
        })
        assertEquals(capped, IncrExResult(10L, 5L))
        assertEquals(reject, IncrExResult(10L, 0L))
        assertEquals(viaFlt, IncrExResult(1.5, 1.5))
      }
    }
  }
}
