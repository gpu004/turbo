# Upstream provenance

- Repository: https://github.com/dmmulroy/anti-slop
- Source commit: `63dbb09f73438e550a6991dc6d778f979ffbf183`
- Copied `index.ts`, `rules/`, `shared/`, and `effect/` from `skills/install-anti-slop/assets/anti-slop`. Upstream tests are not included.
- This repository uses `oxlint` and `@oxlint/plugins` 1.80.0. The upstream checkout uses 1.78.0.
- The Effect plugin (`effect/index.ts`) is registered as `anti-slop-effect`, and all five of its rules are enabled, because this repository depends on Effect directly.
- Local changes: `vendor/eslint-stylistic/UPSTREAM.md` is reworded (commit `53091c9`) and has the same meaning as upstream. Every other vendored file matches the source commit byte for byte.

## Update history

- `c44ef22` → `63dbb09`: three-way merge. The only local change, the reworded vendor `UPSTREAM.md`, was kept, and upstream's one-line edit to that file was ported into the local wording. Adopted upstream #45: `require-readable-spacing` keeps consecutive single-line bindings grouped at any scope, and `createPaddingLineRule` accepts caller-defined statement types. Upstream's `require-readable-spacing.test.ts` passes against this copy on oxlint 1.80.0, and so does `npm run check`. Upstream now recommends [`cloudflare/oxlint-config`](https://github.com/cloudflare/oxlint-config) over anti-slop. This repository has not adopted it.
