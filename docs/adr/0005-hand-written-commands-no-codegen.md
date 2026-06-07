# Hand-written command set, no code generation

Commands are hand-authored Scala, organized by family (strings, hashes, streams, …), rather than generated from the Redis/Valkey `commands.json` specs. Two reasons, equally weighted: (1) precise `Out` types and idiomatic option modeling are the product, and the spec's `reply_schema` is too lossy to generate them — a generator plus curation table is more machinery than it saves; (2) a generator is a second codebase with its own bugs and spec-parsing fragility, made worse as the Redis and Valkey specs diverge post-7.2.

The specs are still used — as a test fixture: a coverage test diffs implemented commands against the server's command list and reports gaps, with an allowlist for deliberately skipped commands (revised by ADR-0012: the list comes from the live server, not vendored `commands.json`). "Full command set" is tracked honestly rather than gated; v1 ships when core families are complete.
