package sage.integration

/**
  * The server images of the integration tests. The coverage spec treats the running binary as the command spec, so every suite must test
  * the exact same release. The benchmarks' `RedisFixture` and the image pre-pull step in `.github/workflows/ci.yml` repeat some of these
  * images, so update them too when you bump one.
  */
object Images {

  val redis: String = "redis:8.8.0"

  val valkey: String = "valkey/valkey:9.1.0"

  // module-bearing Valkey (bundles valkey-json); the stock `valkey` image ships no modules, and `redis` already bundles ReJSON
  val valkeyBundle: String = "valkey/valkey-bundle:9.1.0"

  // a pre-7.2 server (lacks `CLIENT SETINFO`), used only by the bootstrap version-floor test, not by command coverage
  val legacyRedis: String = "redis:6.2"

  // a pre-6.0 server without `HELLO`, used only by the RESP2 fallback tests
  val preHelloRedis: String = "redis:5.0.14"
}
