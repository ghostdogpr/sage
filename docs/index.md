---
layout: home

hero:
  name: "Sage"
  text: "A Redis & Valkey client for Scala 3"
  tagline: Native Scala clients for plain blocking code, Ox, ZIO, Cats Effect, Kyo, and Pekko.
  image:
    light: /sage.svg
    dark: /sage-dark.svg
    alt: Sage
  actions:
    - theme: brand
      text: Getting started
      link: /getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/ghostdogpr/sage

features:
  - title: Native backend types
    details: Choose the direct artifact for plain blocking code, or an Ox, ZIO, Cats Effect, Kyo, or Pekko artifact with its ecosystem's effect and stream types.
  - title: Redis protocol in Scala
    details: Sage implements RESP3, commands, and codecs directly in Scala 3 instead of wrapping a Java client.
  - title: Redis and Valkey support
    details: "Redis 8+ and Valkey 8+ with auto-pipelining, transactions, cluster, sharded pub/sub, client-side caching, and TLS."
---
