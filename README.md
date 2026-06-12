# <img src="docs/public/sage.svg" height="34" alt="" valign="middle">&nbsp; Sage

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**Sage** is a **modern [Scala 3](https://www.scala-lang.org/) client** for **[Redis](https://redis.io)** and **[Valkey](https://valkey.io)**.

It is designed to be **native**, meaning there is no Java client wrapped underneath: the wire protocol, commands, and codecs are implemented directly in Scala.

It is built as a **pure sans-IO core** (RESP3 protocol, typed commands, codecs) with **zero dependencies**, plus a **runtime written once** against [kyo-compat](https://github.com/getkyo/kyo/tree/main/kyo-compat) and cross-published per backend. This makes it usable from **multiple effect ecosystems** ([ZIO](https://zio.dev), [cats-effect](https://typelevel.org/cats-effect/), [Kyo](https://getkyo.io), and [Ox](https://ox.softwaremill.com)), each with its own native types and no wrapper visible.

It targets **RESP3** and modern **Redis 8+ / Valkey 8+**, with automatic pipelining, typed commands composable into pipelines and `MULTI`/`EXEC` transactions, standalone and cluster behind one client type, pub/sub (including sharded), client-side caching, and TLS and ACL auth.

It is available for Scala 3.3.x LTS and later versions, and requires JDK 21+.
