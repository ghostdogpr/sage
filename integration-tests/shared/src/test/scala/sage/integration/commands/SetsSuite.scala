package sage.integration.commands

import kyo.compat.*

import sage.commands.ScanCursor
import sage.integration.BothServersSuite

class SetsSuite extends BothServersSuite {

  clientTest("SADD SCARD SMEMBERS SISMEMBER SMISMEMBER and SREM manage membership") { client =>
    client.sAdd("set-basic", "a", "b", "c").is(3L) >>
      client.sAdd("set-basic", "a").is(0L) >>
      client.sCard("set-basic").is(3L) >>
      client.sMembers[String]("set-basic").is(Set("a", "b", "c")) >>
      client.sIsMember("set-basic", "a").is(true) >>
      client.sIsMember("set-basic", "z").is(false) >>
      client.sMisMember("set-basic", "a", "z", "c").is(Vector(true, false, true)) >>
      client.sRem("set-basic", "a", "z").is(1L) >>
      client.sMembers[String]("set-basic").is(Set("b", "c"))
  }

  clientTest("SPOP and SRANDMEMBER draw members, with and without a count") { client =>
    for {
      _      <- client.sAdd("set-draw", "a", "b", "c", "d")
      _      <- client.sPop[String]("set-draw").satisfies(_.exists(Set("a", "b", "c", "d")))
      popTwo <- client.sPopCount[String]("set-draw", 2L)
      _      <- client.sRandMember[String]("set-draw").satisfies(_.isDefined)
      _      <- client.sRandMemberCount[String]("set-draw", -5L).map(_.size).is(5)
      _      <- client.sPop[String]("set-missing").is(None)
    } yield {
      assertEquals(popTwo.size, 2)
      assert(popTwo.subsetOf(Set("a", "b", "c", "d")))
    }
  }

  clientTest("SMOVE relocates a member between sets") { client =>
    client.sAdd("set-src", "x", "y") >>
      client.sAdd("set-dst", "z") >>
      client.sMove("set-src", "set-dst", "x").is(true) >>
      client.sMove("set-src", "set-dst", "nope").is(false) >>
      client.sMembers[String]("set-src").is(Set("y")) >>
      client.sMembers[String]("set-dst").is(Set("x", "z"))
  }

  clientTest("SDIFF SINTER SUNION and their STORE forms combine sets, SINTERCARD counts") { client =>
    client.sAdd("ops-a", "1", "2", "3") >>
      client.sAdd("ops-b", "2", "3", "4") >>
      client.sDiff[String]("ops-a", "ops-b").is(Set("1")) >>
      client.sInter[String]("ops-a", "ops-b").is(Set("2", "3")) >>
      client.sUnion[String]("ops-a", "ops-b").is(Set("1", "2", "3", "4")) >>
      client.sInterCard("ops-a", "ops-b")().is(2L) >>
      client.sInterCard("ops-a", "ops-b")(limit = Some(1L)).is(1L) >>
      client.sDiffStore("ops-diff", "ops-a", "ops-b").is(1L) >>
      client.sInterStore("ops-inter", "ops-a", "ops-b").is(2L) >>
      client.sUnionStore("ops-union", "ops-a", "ops-b").is(4L) >>
      client.sMembers[String]("ops-union").is(Set("1", "2", "3", "4"))
  }

  clientTest("SSCAN streams members") { client =>
    client.sAdd("set-scan", "a", "b", "c") >>
      client.sScan[String]("set-scan", ScanCursor.start).map(_.items.toSet).is(Set("a", "b", "c"))
  }
}
