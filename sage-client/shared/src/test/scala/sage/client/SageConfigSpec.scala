package sage.client

class SageConfigSpec extends munit.FunSuite {

  private def parsed(uri: String): SageConfig =
    SageConfig.fromUri(uri).fold(problem => fail(problem), identity)

  private def problem(uri: String): String = SageConfig.fromUri(uri).swap.getOrElse(fail(s"expected a Left for $uri"))

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

  test("a stray comma leaves an empty seed that is rejected, never silently dropped") {
    assert(SageConfig.fromUri("redis://a,").isLeft)     // trailing comma would otherwise degrade to standalone a
    assert(SageConfig.fromUri("redis://a,b,").isLeft)   // trailing comma would otherwise pass as a two-seed cluster
    assert(SageConfig.fromUri("redis://a,,").isLeft)    // an interior gap
    assert(SageConfig.fromUri("redis://,a").isLeft)     // a leading gap
    assert(SageConfig.fromUri("redis://[::1],").isLeft) // trailing comma after an IPv6 literal
    assertEquals(problem("redis://alice:hunter2@a,"), "invalid redis URI 'redis://alice:<redacted>@a,': empty host in ''")
  }

  test("a bracketed IPv6 literal is the host, brackets stripped, with or without a port") {
    assertEquals(parsed("redis://[::1]").topology, Topology.Standalone(Endpoint("::1", 6379)))
    assertEquals(parsed("redis://[::1]:6380").topology, Topology.Standalone(Endpoint("::1", 6380)))
    assertEquals(parsed("redis://[2001:db8::1]:6379/2").database, 2)
    assertEquals(
      parsed("redis://[fe80::1]:6379,[fe80::2]:6380").topology,
      Topology.Cluster(Vector(Endpoint("fe80::1", 6379), Endpoint("fe80::2", 6380)))
    )
  }

  test("an unbracketed IPv6 literal or an empty port fails rather than misparsing") {
    assert(SageConfig.fromUri("redis://::1").isLeft)     // ambiguous with host:port, must be bracketed
    assert(SageConfig.fromUri("redis://[::1").isLeft)    // unterminated bracket
    assert(SageConfig.fromUri("redis://[::1]x").isLeft)  // junk after the literal
    assert(SageConfig.fromUri("redis://h:").isLeft)      // colon with no port is not a default
    assert(SageConfig.fromUri("redis://[]:6379").isLeft) // empty host
  }

  test("a cluster URI can select a database when the server supports it") {
    assertEquals(parsed("redis://a,b/2").database, 2)
    assertEquals(parsed("redis://a,b/0").database, 0)
  }

  test("malformed URIs fail with a Left, never throw") {
    assert(SageConfig.fromUri("memcache://h").isLeft)       // unsupported scheme
    assert(SageConfig.fromUri("localhost:6379").isLeft)     // missing scheme
    assert(SageConfig.fromUri("redis://h:0").isLeft)        // port out of range
    assert(SageConfig.fromUri("redis://h:abc").isLeft)      // non-numeric port
    assert(SageConfig.fromUri("redis://h/x").isLeft)        // non-numeric database
    assert(SageConfig.fromUri("redis://h?ssl=true").isLeft) // query params unsupported
  }

  test("a parse error never echoes the password back, even when the URI is the kind that fails to parse") {
    assert(!problem("redis://alice:hunter2@h:0").contains("hunter2"))
    assert(!problem("redis://:hunter2@h:0").contains("hunter2"))
    assert(!problem("redis://:p%40ss@h?x=1").contains("p%40ss"))
    assert(!problem("memcache://alice:hunter2@h").contains("hunter2")) // unsupported scheme still parses far enough to leak
    assert(!problem("alice:hunter2@h").contains("hunter2"))            // missing scheme
    assert(!problem("redis://alice:hun/ter2@h").contains("hun/ter2"))  // unencoded '/' in the password
    assertEquals(problem("redis://alice:hunter2@h:0"), "invalid redis URI 'redis://alice:<redacted>@h:0': invalid port in 'h:0'")
  }
}
