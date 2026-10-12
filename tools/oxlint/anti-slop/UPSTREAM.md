# Upstream provenance

- Repository: https://github.com/dmmulroy/anti-slop
- Source commit: `c44ef22ca116d0ba62a3ff663a0bd13a3f3fa40b`
- Installed paths: `index.ts`, `rules/`, `shared/`, and `effect/`, copied from `skills/install-anti-slop/assets/anti-slop` (the implementation source without upstream tests).
- Dependency compatibility: this repository uses `oxlint` and `@oxlint/plugins` 1.80.0; the upstream checkout uses 1.78.0.
- Intentional deviation: Effect plugin files are included in the vendored copy but are not registered or enabled because this repository has no direct Effect dependency.
