package sage.client

class SageConfigSpec extends munit.FunSuite {

  private def parsed(uri: String): SageConfig =
    SageConfig.fromUri(uri).fold(problem => fail(problem), identity)

  test("a single host yields a standalone topology, default port 6379") {
    assertEquals(parsed("redis://localhost").topology, Topology.Standalone(Endpoint("localhost", 6379)))
    assertEquals(parsed("redis://cache.internal:6380").topology, Topology.Standalone(Endpoint("cache.internal", 6380)))
  }

  test("rediss selects TLS with system trust; redis leaves it off") {
    assertEquals(parsed("rediss://h").tls, Some(TlsConfig(TrustSource.System)))
    assertEquals(parsed("redis://h").tls, None)
  }

  test("userinfo becomes auth, with the default user when only a password is given") {
    assertEquals(parsed("redis://alice:secret@h").auth, Some(AuthConfig("secret", "alice")))
    assertEquals(parsed("redis://:secret@h").auth, Some(AuthConfig("secret", "default")))
    assertEquals(parsed("redis://h").auth, None)
  }

  test("credentials are percent-decoded, with + left literal and an encoded : not splitting") {
    assertEquals(parsed("redis://:p%40ss@h").auth, Some(AuthConfig("p@ss", "default")))  // %40 -> @
    assertEquals(parsed("redis://us%65r:a%3Ab@h").auth, Some(AuthConfig("a:b", "user"))) // %3A -> : inside the password
    assertEquals(parsed("redis://:a+b@h").auth, Some(AuthConfig("a+b", "default")))      // '+' stays '+', not a space
    assertEquals(parsed("redis://:%E2%82%AC@h").auth, Some(AuthConfig("€", "default")))  // multi-byte UTF-8 (€)
  }

  test("malformed percent-encoding in credentials fails with a Left") {
    assert(SageConfig.fromUri("redis://:%zz@h").isLeft)
    assert(SageConfig.fromUri("redis://:%4@h").isLeft)
    assert(SageConfig.fromUri("redis://:%@h").isLeft)
  }

  test("a /db path sets the database") {
    assertEquals(parsed("redis://h:6379/3").database, 3)
    assertEquals(parsed("redis://h").database, 0)
  }

  test("comma-separated hosts yield cluster seeds") {
    assertEquals(
      parsed("redis://a:6379,b:6380,c").topology,
      Topology.Cluster(Vector(Endpoint("a", 6379), Endpoint("b", 6380), Endpoint("c", 6379)))
    )
  }

  test("a cluster URI cannot select a non-zero database") {
    assert(SageConfig.fromUri("redis://a,b/2").isLeft)
    assert(SageConfig.fromUri("redis://a,b/0").isRight)
  }

  test("malformed URIs fail with a Left, never throw") {
    assert(SageConfig.fromUri("memcache://h").isLeft)       // unsupported scheme
    assert(SageConfig.fromUri("localhost:6379").isLeft)     // missing scheme
    assert(SageConfig.fromUri("redis://h:0").isLeft)        // port out of range
    assert(SageConfig.fromUri("redis://h:abc").isLeft)      // non-numeric port
    assert(SageConfig.fromUri("redis://h/x").isLeft)        // non-numeric database
    assert(SageConfig.fromUri("redis://h?ssl=true").isLeft) // query params unsupported
  }
}
