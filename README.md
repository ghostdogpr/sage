# Sage

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**Sage** is a **modern, Scala 3-only client for [Redis](https://redis.io) and [Valkey](https://valkey.io)** — native (no Java client wrapper) and usable from **multiple effect ecosystems**: [ZIO](https://zio.dev), [cats-effect](https://typelevel.org/cats-effect/), [Kyo](https://getkyo.io), and [Ox](https://ox.softwaremill.com).

It is built as a **pure sans-IO core** (RESP3 protocol, typed commands, codecs — zero dependencies) plus a **runtime written once** against [kyo-compat](https://github.com/getkyo/kyo/tree/main/kyo-compat) and cross-published per backend, so each ecosystem gets its native types with no wrapper visible.

> 🚧 **Work in progress** — sage is under active development and not yet released.

## Highlights (planned for v1)

- **RESP3 only** — typed replies, push frames; targets modern Redis (8+) and Valkey (8+)
- **Automatic pipelining** — commands from all fibers multiplexed onto one connection
- **Typed commands as values** — composable into pipelines and `MULTI`/`EXEC` transactions with typed results
- **Standalone and cluster** behind one client type — topology is configuration
- **Pub/sub** (including sharded) as native streams per backend
- **Client-side caching** — invalidation-driven local reads, opt-in per call
- **TLS and ACL auth** out of the box

## Design

The design is documented in the repo: [`CONTEXT.md`](CONTEXT.md) (glossary), [`docs/adr/`](docs/adr/) (architecture decision records), and [`docs/PRD.md`](docs/PRD.md).

## Requirements

- Scala 3.3+ (LTS)
- JDK 21+
