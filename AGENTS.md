# AGENTS.md

## When you are corrected, fix the repository

A correction you only remember is lost next session. Put it in the cheapest layer that can hold it, and stop at the first one that works:

1. **Codebase.** Change types, APIs, or data structures so the wrong shape can't be written. Examples: branded ids and the clock in `src/domain.ts`, the `PendingJob | LeasedJob` union in `src/model.ts`, route params derived from the pattern in `src/router.ts`.
2. **Static analysis.** If it can still be written, make CI fail on it. Add a `@ts-expect-error` case to `test/types.test.ts`, a lint rule with a fixture in `tools/oxlint/turbo/`, or a property to `test/model.property.test.ts`.
3. **Verification.** If it is about stored state or concurrency, check it. `invariantViolation` in `src/model.ts` runs on every R2 load and before every write. `spec/QueueBroker.tla` model-checks the commit protocol.
4. **Prose.** If none of those can express it, add a line below and say why code can't hold it.
5. **Human review.** This is not a place to keep a rule. If a reviewer catches the same mistake twice, move it up this list.

A turbo lint rule bans an operation everywhere and is turned off only for the module that owns it, listed in `RULE_OWNERS` in `tools/oxlint/turbo/index.ts`. If your code trips one, move the code into the owning module. Don't add an override.

## Rules code can't enforce yet

- `QueueFile` in `src/model.ts` is a stored format that running brokers read from R2. You can add an optional field. Renaming or removing a field, or changing what it means, needs a new `version` and a reader for the old one. The schema can check a file's shape, but not whether a change is compatible with files already stored.
- If you change how `src/broker.ts` loads, caches, or writes the queue, update `spec/QueueBroker.tla` in the same change. Nothing links the spec to the code, so they can drift apart unnoticed.
- `readme.md` is the API contract for clients. Update it when a route, status code, response shape, or guarantee changes.

## Commands

```sh
npm run check       # lint, lint-rule tests, format, Wrangler dry run, tsc
npm test            # unit, property, type, and Worker tests
npm run verify:tla  # TLC model check of spec/QueueBroker.tla (needs Java)
```
