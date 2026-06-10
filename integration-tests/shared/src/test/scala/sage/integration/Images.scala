package sage.integration

/**
  * The single source for server images: the coverage spec treats the running binary as the command spec, so every suite must test the
  * exact same release. Bump both here and nowhere else.
  */
object Images {

  val redis: String = "redis:8.8.0"

  val valkey: String = "valkey/valkey:9.1.0"
}
