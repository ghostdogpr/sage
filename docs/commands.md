# Commands & codecs

<!--
WRITER NOTES — replace this body with real content.
Ground snippets in: examples/*/CommandsExample.scala and examples/shared/Domain.scala.

Purpose: how you actually issue commands, and how user types cross the wire.

Sections to cover (grounded in CONTEXT.md glossary):

1. The Commands facade — `Commands.get`, `Commands.incr`, … yield `Command` values,
   named one-for-one with the client's methods. A `Command[Out]` is a pure value
   (wire encoding + typed reply decoder); the client's per-command methods are sugar
   over it. Show both styles: client.get("k") vs run(Commands.get("k")).

2. Families — commands mirror the server's documented groups (strings, keys, hashes,
   sets, sorted sets, …). The per-family objects are internal; `Commands` is the
   public entry point.

3. Codecs — KeyCodec / ValueCodec convert user types to/from wire bytes at a command
   boundary. Built-in codecs decode STRICTLY (non-canonical bytes fail, not coerced).
   Keys must be hashable to cluster slots. Show the hand-written ValueCodec for `User`
   from examples/shared/Domain.scala.

4. (Optional) note that a Command value is reusable across run / pipelines /
   transactions — sets up the next pages.
-->
