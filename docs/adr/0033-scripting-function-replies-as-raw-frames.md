# Scripting and function replies returned as raw RESP3 frames

`EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO` and `FCALL`, `FCALL_RO` return `Command[Frame]` — the raw RESP3 value — because a script's or function's reply shape is defined by user code and is unknowable to sage. This is the one deliberate exception to "every command has a precise typed result" (PRD story 7): everywhere else `Out` is concrete, but here sage returns the wire ADT and the caller `.map`s a decoder when it knows its own script's shape. `Frame` is already a public, sealed, structural-equality ADT, so no parallel type is invented. Top-level script errors (`redis.error_reply`, runtime errors) surface as server-error `SageException`s through `Reply.run`, so only successful replies reach `decode`.

`numkeys` is computed from the supplied key list — never passed by the caller — and `keyIndices` is derived (`Vector.range(2, 2 + keys.length)`), so the existing cluster routing and CROSSSLOT machinery applies unchanged and a zero-key script is simply keyless. Keys take `KeyCodec`, args take `ValueCodec`. There is no automatic `EVALSHA`→`EVAL` fallback: that is hidden retry (ADR-0006's territory), and because Commands are composable values a caller writes the fallback itself (`run(evalSha(...)).orElse(run(eval(...)))`).

## Considered Options

- **A new user-facing reply ADT** mirroring `Frame`'s data cases — pure duplication of an already-public type plus a translation layer to maintain forever.
- **A typeclass-decoded generic** `eval[R](...)(using ReplyDecoder[R])` — fights the inherently dynamic reply and still needs a raw escape hatch.
- **Return raw `Frame` (chosen)** — `Frame` is already the clean public RESP3 ADT; the caller decodes via `.map`.

## Consequences

`Frame` becomes part of the public command surface (previously only the internal `Connection.exec` returned it). The script-management commands keep precise typed results (`SCRIPT LOAD` → SHA `String`, `SCRIPT EXISTS` → `Vector[Boolean]`, `FUNCTION LOAD` → library-name `String`, `FUNCTION LIST`/`STATS` → typed ADTs); only the script-/function-output commands are untyped, and that untyped-ness is contained to six builders.
