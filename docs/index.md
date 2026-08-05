---
layout: home

hero:
  name: "Sage"
  text: "A Redis & Valkey client for Scala 3"
  tagline: One client for any Scala stack, built on a from-scratch native Redis protocol implementation.
  image:
    light: /sage.svg
    dark: /sage-dark.svg
    alt: Sage
  actions:
    - theme: brand
      text: Getting Started
      link: /getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/ghostdogpr/sage

features:
  - title: Use any Scala stack
    details: First-class ZIO, Cats Effect, Kyo, Ox, and Pekko artifacts that use each ecosystem's native types.
  - title: Fast, native Redis protocol
    details: RESP3, commands, and codecs implemented directly in Scala 3 rather than through a wrapped Java client.
  - title: Modern and feature-rich
    details: "Redis 8+ and Valkey 8+ with auto-pipelining, transactions, cluster, sharded pub/sub, client-side caching, and TLS."
---
