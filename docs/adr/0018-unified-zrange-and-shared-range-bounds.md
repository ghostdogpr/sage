# Unified ZRANGE and shared range bounds

Modern Redis (6.2+) folded `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, and `ZREVRANGEBYLEX` into `ZRANGE` with `BYSCORE`/`BYLEX`/`REV`/`LIMIT` options and deprecated all five. Sage exposes a single `zRange[K, V](key, range: ZRange[V])` over a sealed `ZRange` — `ByRank(start, stop, rev)`, `ByScore(min, max, limit, rev)`, `ByLex(min, max, limit, rev)` — so the per-mode legal option space is type-enforced (a `LIMIT` on a by-rank query is unrepresentable), and the five deprecated commands move to `Coverage.skipped` with the reason `deprecated: ZRANGE with BYSCORE/BYLEX/REV`, consistent with the existing `GETSET`/`RPOPLPUSH`/`HMSET` precedent. `WITHSCORES` changes the reply type and so is the separate builder `zRangeWithScores` (ADR-0011); `ZRANGESTORE` keeps its own builder taking the same `ZRange`.

`min`/`max` are always given low→high regardless of direction; the encoder emits them in descending wire order under `REV` for `BYSCORE`/`BYLEX`, so the raw command's notorious bound-swap foot-gun cannot be expressed.

## Score and lex bounds are shared domain primitives

`ScoreBoundary` (`Inclusive` / `Exclusive` / `NegInf` / `PosInf`) and `LexBoundary` (`Inclusive` / `Exclusive` / `Min` / `Max`) are reused across `zRange`, `zRangeStore`, `zCount`/`zLexCount`, and `zRemRangeByScore`/`zRemRangeByLex`. This is the same exception ADR-0011 carves out for `ListSide`: a score or lex boundary is one domain primitive with one legal space everywhere it occurs, not a per-command option group — so sharing it is not the cross-command enum reuse the ADR otherwise forbids.

## Considered Options

- **Unified `zRange` + skip the five (chosen)** — one type-safe range model, five deprecated commands acknowledged as skipped. Matches the modern-only stance and the existing deprecation precedent.
- **Implement all six builders 1:1** — full wire fidelity, no skips, but contradicts the modern-only posture and duplicates the range and boundary modeling six ways.
