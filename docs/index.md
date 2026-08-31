---
layout: home

hero:
  name: "Sage"
  text: "A Redis & Valkey client for Scala 3"
  tagline: Native Scala clients for ZIO, Cats Effect, Kyo, Ox, and Pekko.
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
    details: Choose a ZIO, Cats Effect, Kyo, Ox, or Pekko artifact. Each one uses its ecosystem's effect and stream types.
  - title: Redis protocol in Scala
    details: Sage implements RESP3, commands, and codecs directly in Scala 3 instead of wrapping a Java client.
  - title: Redis and Valkey support
    details: "Redis 8+ and Valkey 8+ with auto-pipelining, transactions, cluster, sharded pub/sub, client-side caching, and TLS."
---
