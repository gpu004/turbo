# Upstream provenance

- Repository: https://github.com/dmmulroy/anti-slop
- Source commit: `c44ef22ca116d0ba62a3ff663a0bd13a3f3fa40b`
- Copied `index.ts`, `rules/`, `shared/`, and `effect/` from `skills/install-anti-slop/assets/anti-slop`. Upstream tests are not included.
- This repository uses `oxlint` and `@oxlint/plugins` 1.80.0. The upstream checkout uses 1.78.0.
- The vendored copy includes Effect plugin files. They are not registered or enabled because this repository has no direct Effect dependency.
