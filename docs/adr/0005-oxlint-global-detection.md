# ADR 0005: Lexical-Scope Global Detection in Custom Oxlint Rules

## Context

`tools/oxlint/turbo/index.ts` enforces module ownership rules such as `turbo/no-ambient-effects`, which bans `Date`, `crypto.randomUUID`, `crypto.getRandomValues`, and `Math.random` outside `src/domain.ts`.

## Decision

`isGlobal` first checks `sourceCode.isGlobalReference(node)` and then walks the lexical scope chain (`sourceCode.getScope(node)` up to `null`), treating an identifier with no local definition as a global reference.

Cloudflare Workers runtime globals such as `crypto` are not included in Oxlint's `builtin` environment, so `isGlobalReference` alone returns `false` for them. Walking the scope chain catches unshadowed runtime globals while still allowing locally shadowed parameters or bindings (tested in `tools/oxlint/turbo/fixtures/no-ambient-effects.ts`).

## Consequences

- Rules catch Workers runtime globals without duplicating ambient global lists in `.oxlintrc.json`.
- Locally shadowed identifiers with the same name remain allowed.
