# Docs site — handoff

This is the skeleton for the sage documentation site (issue #46): a VitePress site
under `docs/`, deployed to GitHub Pages, mirroring the proteus/purelogic setup. The
**infrastructure is done and builds**; the **content pages are stubs** holding writer
notes. Your job: replace each page's `<!-- WRITER NOTES … -->` block with real prose
and snippets.

> Delete this file once the content is written — it is not a published page (the
> `docs:build` includes only what's in the sidebar/nav, but don't leave it lying around).

## What's done (don't redo)

| File | State |
| --- | --- |
| `docs/package.json` | ✅ vitepress dep + `docs:dev` / `docs:build` / `docs:preview` |
| `docs/.vitepress/config.mts` | ✅ base `/sage/`, nav, sidebar, logo, umami, GitHub link, local search |
| `docs/.vitepress/theme/index.js` + `custom.css` | ✅ hero color `#CB4239` (from the logo) |
| `docs/public/sage.svg` + `favicon.png` | ✅ already present (the logo) |
| `docs/index.md` | ✅ **finalized** landing hero + 3 feature cards — content complete |
| `.github/workflows/docs.yml` | ✅ build + Pages deploy on release / manual dispatch, with `@VERSION@` substitution |

## What needs writing (the stubs)

Each file has detailed per-section notes inside it. Order to write them:

1. `getting-started.md` — single entry point: what sage is, install coordinates per backend, connect (tabbed), the "how it works" aside, and a short tour. **This page carries the connection-model explanation** (as a "how it works" aside, not a heading). There is no separate Quickstart page (merged in to avoid two synonymous entry pages, matching proteus/purelogic).
2. `commands.md` — the `Commands` facade, families, codecs.
3. `pipelines-transactions.md` — Pipeline vs Transaction (the crisp distinction).
4. `pubsub.md` — classic + sharded.
5. `client-side-caching.md` — Cached Reads (per-call opt-in).
6. `deployments.md` — standalone / cluster / master-replica / ReadFrom / TLS & ACL (config, not code).
7. `faq.md` — Q&A list.
8. `about.md` — short about page.

## Decisions already locked (honor these)

- **Backends:** ZIO, cats-effect, Kyo, Ox. Coordinates (confirmed against `build.sbt`):
  `sage-client-zio`, `sage-client-ce`, `sage-client-kyo`, `sage-client-ox`, all under
  `com.github.ghostdogpr`, `%%`, version `@VERSION@`. `sage-core` is transitive — users
  depend on the backend artifact only. Requires JDK 21+, Scala 3.3.x LTS+.
- **Snippet style:** construction/connect is a **code-group with a tab per backend**;
  every other (command) snippet is shown **once in Ox direct style** (reads like plain
  Scala). Do not repeat command snippets four times.
- **No API / Scaladoc link** — deliberately omitted from nav (Scaladoc isn't hosted).
  Don't add one.
- **Connection model + auto-pipelining** live inside `getting-started.md` as a "how it
  works" aside. Auto-pipelining is **never** a heading next to "Pipelines" — they're
  different concepts (a transparent property vs a value you build).
- **`@VERSION@`** is the literal placeholder in install snippets; `docs.yml` sed-replaces
  it at deploy. Leave it as-is.

## Grounding — reuse, don't invent

Every code snippet must reflect real, compiling usage. Two sources of truth:

- **`examples/`** — runnable per-backend examples. Map (see each stub for specifics):
  - `*/Tour.scala` → getting-started construction (tabbed) + tour
  - `*/CommandsExample.scala` + `shared/Domain.scala` → commands
  - `*/PipelinesExample.scala`, `*/TransactionsExample.scala` → pipelines & transactions
  - `*/PubSubExample.scala`, `zio/ClusterExample.scala` → pub/sub
  - `*/CachedReadsExample.scala` → client-side caching
  - `zio/ClusterExample.scala`, `ox/MasterReplicaExample.scala`, `ce/TlsExample.scala` → deployments
  - Read `examples/README.md` first — it explains the tour/spotlight split.
- **`CONTEXT.md`** (repo root) — the glossary. Use its exact terms (Multiplexed/Dedicated/
  Subscription Connection, Auto-pipelining, Pipeline, Transaction, Cached Read, Shard
  Channel, Read Policy, …) and respect its `_Avoid_` lists. The "Example dialogue" at the
  bottom is good raw material for prose and the FAQ.

## Writing rules

- **Never reference ADRs, the PRD, or issue numbers in rendered docs.** State rationale
  in plain words. (The stub notes mention ADR files only to point you at background;
  don't carry those references into the prose.)
- **Avoid em dashes (`—`) in prose** — they read as AI-generated. Use commas, colons,
  parentheses, or separate sentences instead.
- Match the tone/length of proteus & purelogic docs (`~/GIT/proteus/docs`,
  `~/GIT/purelogic/docs`) — concise, example-led. Use VitePress containers (`::: tip`,
  `::: warning`) and code-groups (` ```scala [ZIO] `) as they do.
- No AI attribution anywhere (commits, PRs) — see `CLAUDE.md`.

## Build & preview

```sh
cd docs
npm install
npm run docs:dev      # local dev server with hot reload
npm run docs:build    # production build — must pass for acceptance
npm run docs:preview  # serve the built site
```

Snippets won't be type-checked by the build, so verify each against the example file it
came from (open the source, copy, adapt the effect flavor only as the style rule allows).

## Acceptance criteria (from issue #46)

- [ ] `npm run docs:build` succeeds
- [ ] `docs.yml` deploys to Pages on release + manual dispatch, with `@VERSION@` substitution ✅ (skeleton)
- [ ] Pages present: landing, getting started (install + tour + connection-model aside), feature guides (commands, pipelines & transactions, pub/sub, client-side caching), deployments
- [ ] Snippets cross-checked against `examples/`
- [ ] Nav/sidebar + GitHub social link configured ✅ (skeleton)
