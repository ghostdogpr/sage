# Client-side caching

<!--
WRITER NOTES — replace this body with real content.
Ground snippets in: examples/*/CachedReadsExample.scala.
Background: docs/adr/0028-client-side-caching-optin-tracking-and-per-connection-cache.md
(do NOT reference the ADR in the rendered docs — state the behavior in plain words).

Purpose: how to serve reads from a local cache with explicit opt-in.

Sections to cover (grounded in CONTEXT.md glossary):

1. Cached Read — a read command executed with explicit PER-CALL opt-in to client-side
   caching: served from the local cache until a server invalidation push or TTL evicts
   it. Emphasize it is opt-in per call, not a global mode.

2. Show the API from CachedReadsExample.scala — the opt-in form on a read, and what
   you get back.

3. Invalidation — server push invalidates entries; explain staleness window is bounded
   by invalidation + TTL. Keep it conceptual; deep tracking-protocol detail belongs in
   the API docs, not here.
-->
