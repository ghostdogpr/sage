---
layout: home

hero:
  name: "Sage"
  text: "A Redis & Valkey client for Scala 3"
  tagline: One client for any effect system, built on a from-scratch native Redis protocol implementation.
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
  - title: Use any effect system
    details: First-class ZIO, cats-effect, Kyo, and Ox artifacts, each with its ecosystem's native types and no wrapper visible.
  - title: Native Redis protocol
    details: RESP3, commands, and codecs implemented directly in Scala 3, with no Java client wrapped underneath.
  - title: Modern and feature-rich
    details: "Redis 8+ and Valkey 8+ with auto-pipelining, transactions, cluster, sharded pub/sub, client-side caching, and TLS."
---
