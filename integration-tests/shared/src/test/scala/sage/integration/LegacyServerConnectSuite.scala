package sage.integration

import kyo.compat.*

/**
  * Bootstrap must come up against a pre-7.2 server that rejects `CLIENT SETINFO`. Redis-only by design: Valkey has no pre-7.2 release.
  */
class LegacyServerConnectSuite extends ServerSuite(Images.legacyRedis) {

  test("connects and round-trips against a pre-7.2 server that lacks CLIENT SETINFO") {
    withClient { client =>
      for {
        _     <- client.set("legacy", "ok")
        value <- client.get[String]("legacy")
      } yield assertEquals(value, Some("ok"))
    }
  }
}
