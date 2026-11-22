---
name: delete-comment
description: "Audit and eliminate code comments across the repository (or a targeted path): classify each comment as Tautology, Stale, Excuse, or Decision Record; delete tautologies/stale comments, file issues for excuses, move decision records into docs/adr/, and tighten the turbo/no-comments ratchet in .oxlintrc.json. Use for /delete-comment or whenever cleaning up comments or tightening the comment ratchet."
disable-model-invocation: true
---

# Delete comments and tighten the ratchet

Audit every code comment in the target scope (the whole codebase by default, or the files/directories the user names), dispose of every comment properly, and lock the gains into `.oxlintrc.json`'s `turbo/no-comments` ratchet.

## 1. Inventory comments in scope

Inspect all non-generated source, test, and config files in scope (in this repo: `src/`, `test/`, `vitest.config.ts`, and `tools/oxlint/turbo/`). Exclude vendored third-party code (`tools/oxlint/anti-slop/**`) and generated files (`worker-configuration.d.ts`).

Ignore machine-read directives that tooling requires:
- `// @ts-expect-error ...` (TypeScript negative type tests in `test/types.test.ts`)
- `// SAFETY: ...` or `/* SAFETY: ... */` (required by `anti-slop/require-safety-comment-for-type-assertion` on non-const type assertions)
- `// expect: turbo/<rule>` (fixture markers read by `tools/oxlint/turbo/test.ts`)
- Shebangs (`#!...`)

Note: Never treat `eslint-disable`, `oxlint-disable`, `@ts-ignore`, or `@ts-nocheck` as exempt directives.

Count the total comment lines in scope before making edits so you can report the exact lines shed (and remaining lines if doing a partial ratchet pass).

## 2. Classify every comment

Assign each comment block to exactly one category:

- **Tautology** — repeats what is already visible from types, names, control flow, tests, or lint rules.
- **Stale** — references outdated code layout, old implementations, or behavior that no longer holds.
- **Excuse** — rationalizes a hack, brittle workaround, timing fudge, or shortcut so it slips past review and spreads.
- **Decision Record** — explains a genuine, non-obvious architectural, platform, concurrency, or compatibility constraint that the code itself cannot express.

## 3. Take action by category

- **Tautology / Stale**: Delete the comment.
- **Excuse**: Delete the comment AND file an issue in the project tracker describing the hack, why it exists, why it is harmful, and the suggested fix. (Link the issue if issue-linking tools are available.)
- **Decision Record**: Move the rationale into an Architecture Decision Record under `docs/adr/` (creating a new numbered ADR `docs/adr/NNNN-<slug>.md` or extending a closely related existing ADR, and updating `docs/adr/README.md`), then delete the comment from the code.

If a `SAFETY:` comment spans multiple `//` lines, collapse it into a single `/* SAFETY: ... */` block comment or single-line `// SAFETY: ...` token so continuation lines do not trip `turbo/no-comments`.

## 4. Tighten the `turbo/no-comments` ratchet

`.oxlintrc.json` configures `turbo/no-comments` with a per-file budget map:

```json
"turbo/no-comments": [
  "error",
  {
    "maxCommentsByFile": {}
  }
]
```

- Any file omitted from `maxCommentsByFile` has budget `0` (all non-directive comments are errors).
- For any file where comments remain after a partial pass, set `maxCommentsByFile["path/to/file.ts"]` to the exact count of remaining non-directive comment tokens — never higher than before.
- Whenever comments are removed from a ratcheted file, lower its entry to the new count, and delete the entry once it reaches `0`. The rule itself errors if a file drops below its configured budget so the ratchet always tightens.
- Never add an `.oxlintrc.json` `"off"` override for `turbo/no-comments`; `RULE_OWNERS["no-comments"]` is `[]` and `npm run lint:rules` rejects overrides.

## 5. Verify and report

Run the repository checks:

```sh
npm run check
npm test
```

Report back with:
1. A breakdown of every comment processed by category (Tautology, Stale, Excuse, Decision Record) and file.
2. Links to any issues filed for Excuses and any ADRs created/updated.
3. Ratchet changes in `.oxlintrc.json` (previous vs. new budgets).
4. Exact lines of code shed in this pass (and remaining comment lines if any ratcheted files remain).
