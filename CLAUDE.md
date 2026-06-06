# Working on sage

## Comments

Default to zero comments; when unsure, omit. Comment only what is non-obvious and cannot be expressed in code: contracts (copy vs zero-copy, mutation rules, poisoning), surprising design decisions, and protocol subtleties. One or two lines, not paragraphs.

Never:

- comments that restate what the code or signature already says
- references to ADRs, the PRD, or issues — state the rationale in plain words or not at all
- comments in tests or build files unless something is genuinely surprising
- notes about future plans ("will grow later", "arrives in a later slice")

## Commits and PRs

Never add AI attribution anywhere: no `Co-Authored-By: Claude` in commit messages, no "Generated with Claude Code" in PR descriptions. Commits and PRs must show only the repository owner.
