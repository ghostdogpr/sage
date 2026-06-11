package sage.integration.commands

import kyo.compat.*

import sage.integration.{Images, ServerSuite}

/**
  * DELIFEQ is a Valkey-only atomic compare-and-delete absent in Redis, so it has no cross-server counterpart (ADR-0026).
  */
class ValkeyKeysExtrasSuite extends ServerSuite(Images.valkey) {

  test("DELIFEQ deletes only when the current value matches") {
    withClient { client =>
      for {
        _        <- client.set("vk-lock", "token-1")
        mismatch <- client.delIfEq("vk-lock", "other")
        present  <- client.exists("vk-lock")
        matched  <- client.delIfEq("vk-lock", "token-1")
        gone     <- client.exists("vk-lock")
        missing  <- client.delIfEq("vk-lock-absent", "x")
      } yield {
        assertEquals(mismatch, false)
        assertEquals(present, 1L)
        assertEquals(matched, true)
        assertEquals(gone, 0L)
        assertEquals(missing, false)
      }
    }
  }
}
