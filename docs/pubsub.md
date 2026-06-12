# Pub/Sub

<!--
WRITER NOTES — replace this body with real content.
Ground snippets in: examples/*/PubSubExample.scala (classic) and
examples/zio/ClusterExample.scala (sharded pub/sub spotlight).

Purpose: how to subscribe and publish, classic and sharded, and the connection
isolation that keeps a slow consumer from stalling commands.

Sections to cover (grounded in CONTEXT.md glossary):

1. Classic pub/sub — channel and pattern subscriptions surface as a Message
   (channel + payload); a pattern subscription yields a Pattern Message (also names
   the matched glob). All classic subscriptions share the one Subscription Connection,
   created lazily; a slow consumer backpressures it at the TCP level (lossless) and
   can stall peer subscriptions but NEVER the Multiplexed Connection's commands.
   Show subscribe → stream of Messages → publish. Source: PubSubExample.scala.

2. Sharded pub/sub — Shard Channels: SSUBSCRIBE/SPUBLISH target the slot's owning Node;
   messages stay within the shard (no pattern form). Carried on per-owning-Node Sharded
   Subscription Connections; re-homed on slot migration / failover. Surfaces as an
   ordinary Message. This is a cluster feature — source: ClusterExample.scala (zio).

3. Note the distinction: a Message (pub/sub delivery) is NOT a Frame (wire value) and
   NOT a Stream Entry (a record in a Stream).
-->
