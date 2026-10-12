# Vendored padding-line-between-statements

Source: [ESLint Stylistic](https://github.com/eslint-stylistic/eslint-stylistic), commit `435c3ea0fd26a5fef9042c4b36b6e165fbbf8d08`.

Copied files:

- Copied `packages/eslint-plugin/rules/padding-line-between-statements/padding-line-between-statements.ts`.
- Copied `packages/eslint-plugin/rules/padding-line-between-statements/types.d.ts` as `padding-line-options.d.ts`.
- Kept the root `LICENSE` verbatim. Both OpenJS Foundation and ESLint Stylistic notices apply.

The rule is MIT-licensed. Keep `LICENSE` with every redistributed copy, including skill assets. The rule requires no Stylistic, ESLint, TypeScript-ESLint, or additional parser runtime dependency.

## Local adaptations

- Replace upstream type aliases with Oxlint's ESTree, context, token, comment, and rule types. Upstream's token type includes comments. Oxlint exposes them separately.
- Replace `AST_NODE_TYPES` enum members with identical string literals.
- Guard indexed reads for repositories that use `noUncheckedIndexedAccess`. Missing AST or configuration entries that should always exist raise invariant errors instead of using new non-null assertions.
- Replace the repository-specific `createRule` factory with `createPaddingLineRule(options)`. The anti-slop wrapper supplies typed options directly and exposes no user configuration options.
- Implement the required AST helpers in `padding-line-ast.ts` using Oxlint's public source-code and token API. `isParenthesized` checks one pair of parentheses to exclude parenthesized directive strings. It does not need the upstream general-purpose overloads.
- Retain the upstream statement matchers, scope tracking, comment-aware insertion and removal, selector support, and diagnostic text. Keep upstream naming and non-null assumptions in this file so future diffs remain reviewable. Do not use it as a model for new application code.

The spacing policy lives outside this directory in `../../rules/require-readable-spacing.ts`. It adds spacing without collapsing existing blank lines. Short local bindings, consecutive imports, and adjacent overload signatures and their implementation remain grouped. The rule uses syntax to decide where to add spacing, not business logic.

## Updating and verification

Fetch an explicit upstream revision and compare its rule and types with the revision recorded above. Port relevant fixes and keep the adapters above. Update this record and preserve the license. In the anti-slop upstream repository, run `pnpm check` and `pnpm sync:skill-assets` as its guidance requires. In this repository, run `npm run check` and `npm test`.

The anti-slop upstream repository has focused Oxlint RuleTester cases in `rules/require-readable-spacing.test.ts`. They test exact fixes, JSDoc and trailing comments, same-line statements, semicolon-free code, TypeScript exports and overloads, Effect-style generators, and upstream removal behavior. Its `rules/require-readable-spacing-cli.test.ts` tests the exported plugin through the native Oxlint CLI on multiple files. Those tests check rejection, autofix, and whether repeated fixes change the result.

Those test files are not included in this vendored copy. The complete ESLint Stylistic JavaScript and TypeScript test suites have not been ported. The focused tests provide compatibility evidence, not proof of full upstream conformance.
