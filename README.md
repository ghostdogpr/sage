# <img src="docs/public/sage.svg" height="34" alt="" valign="middle">&nbsp; Sage

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**Sage** is a **Redis & Valkey client for Scala 3**: one client for any effect system, built on a from-scratch native Redis protocol implementation.

**Use any effect system.** First-class [ZIO](https://zio.dev), [Cats Effect](https://typelevel.org/cats-effect/), [Kyo](https://getkyo.io), and [Ox](https://ox.softwaremill.com) artifacts, each with its ecosystem's native types and no wrapper visible.

**Fast, native Redis protocol.** RESP3, commands, and codecs implemented directly in Scala 3, with no Java client wrapped underneath and fast by design.

**Modern and feature-rich.** [Redis](https://redis.io) 8+ and [Valkey](https://valkey.io) 8+ with auto-pipelining, transactions, cluster, sharded pub/sub, client-side caching, and TLS.

It is available for Scala 3.3.x LTS and later versions, and requires JDK 21+.

### Consult the [Documentation](https://ghostdogpr.github.io/sage/) to learn how to use Sage.
