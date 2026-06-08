package sage.integration.commands

import kyo.compat.*

import sage.commands.ScanCursor
import sage.integration.{Images, ServerSuite}

abstract class SetsSuite(image: String) extends ServerSuite(image) {

  test("SADD SCARD SMEMBERS SISMEMBER SMISMEMBER and SREM manage membership") {
    withClient { client =>
      for {
        added   <- client.sAdd("set-basic", "a", "b", "c")
        dup     <- client.sAdd("set-basic", "a")
        card    <- client.sCard("set-basic")
        members <- client.sMembers[String, String]("set-basic")
        isA     <- client.sIsMember("set-basic", "a")
        isZ     <- client.sIsMember("set-basic", "z")
        multi   <- client.sMisMember("set-basic", "a", "z", "c")
        removed <- client.sRem("set-basic", "a", "z")
        after   <- client.sMembers[String, String]("set-basic")
      } yield {
        assertEquals(added, 3L)
        assertEquals(dup, 0L)
        assertEquals(card, 3L)
        assertEquals(members, Set("a", "b", "c"))
        assertEquals(isA, true)
        assertEquals(isZ, false)
        assertEquals(multi, Vector(true, false, true))
        assertEquals(removed, 1L)
        assertEquals(after, Set("b", "c"))
      }
    }
  }

  test("SPOP and SRANDMEMBER draw members, with and without a count") {
    withClient { client =>
      for {
        _        <- client.sAdd("set-draw", "a", "b", "c", "d")
        popOne   <- client.sPop[String, String]("set-draw")
        popTwo   <- client.sPopCount[String, String]("set-draw", 2L)
        rndOne   <- client.sRandMember[String, String]("set-draw")
        rndDup   <- client.sRandMemberCount[String, String]("set-draw", -5L)
        emptyPop <- client.sPop[String, String]("set-missing")
      } yield {
        assert(popOne.exists(Set("a", "b", "c", "d")))
        assertEquals(popTwo.size, 2)
        assert(popTwo.subsetOf(Set("a", "b", "c", "d")))
        assert(rndOne.isDefined)
        assertEquals(rndDup.size, 5)
        assertEquals(emptyPop, None)
      }
    }
  }

  test("SMOVE relocates a member between sets") {
    withClient { client =>
      for {
        _      <- client.sAdd("set-src", "x", "y")
        _      <- client.sAdd("set-dst", "z")
        moved  <- client.sMove("set-src", "set-dst", "x")
        absent <- client.sMove("set-src", "set-dst", "nope")
        src    <- client.sMembers[String, String]("set-src")
        dst    <- client.sMembers[String, String]("set-dst")
      } yield {
        assertEquals(moved, true)
        assertEquals(absent, false)
        assertEquals(src, Set("y"))
        assertEquals(dst, Set("x", "z"))
      }
    }
  }

  test("SDIFF SINTER SUNION and their STORE forms combine sets, SINTERCARD counts") {
    withClient { client =>
      for {
        _       <- client.sAdd("ops-a", "1", "2", "3")
        _       <- client.sAdd("ops-b", "2", "3", "4")
        diff    <- client.sDiff[String, String]("ops-a", "ops-b")
        inter   <- client.sInter[String, String]("ops-a", "ops-b")
        union   <- client.sUnion[String, String]("ops-a", "ops-b")
        card    <- client.sInterCard("ops-a", "ops-b")()
        cardLim <- client.sInterCard("ops-a", "ops-b")(limit = Some(1L))
        diffN   <- client.sDiffStore("ops-diff", "ops-a", "ops-b")
        interN  <- client.sInterStore("ops-inter", "ops-a", "ops-b")
        unionN  <- client.sUnionStore("ops-union", "ops-a", "ops-b")
        stored  <- client.sMembers[String, String]("ops-union")
      } yield {
        assertEquals(diff, Set("1"))
        assertEquals(inter, Set("2", "3"))
        assertEquals(union, Set("1", "2", "3", "4"))
        assertEquals(card, 2L)
        assertEquals(cardLim, 1L)
        assertEquals(diffN, 1L)
        assertEquals(interN, 2L)
        assertEquals(unionN, 4L)
        assertEquals(stored, Set("1", "2", "3", "4"))
      }
    }
  }

  test("SSCAN streams members") {
    withClient { client =>
      for {
        _    <- client.sAdd("set-scan", "a", "b", "c")
        page <- client.sScan[String, String]("set-scan", ScanCursor.start)
      } yield assertEquals(page.items.toSet, Set("a", "b", "c"))
    }
  }
}

class RedisSetsSuite extends SetsSuite(Images.redis)

class ValkeySetsSuite extends SetsSuite(Images.valkey)
