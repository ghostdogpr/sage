package sage.integration

/**
  * The single source for server images: the coverage spec treats the running binary as the command spec, so every suite must test the
  * exact same release. Bump both here and nowhere else.
  */
object Images {

  val redis: String = "redis:8.8.0"

  val valkey: String = "valkey/valkey:9.1.0"

  // module-bearing Valkey (bundles valkey-json); the stock `valkey` image ships no modules, and `redis` already bundles ReJSON
  val valkeyBundle: String = "valkey/valkey-bundle:9.1.0"

  // a pre-7.2 server (lacks `CLIENT SETINFO`), used only by the bootstrap version-floor test, not by command coverage
  val legacyRedis: String = "redis:6.2"
}
