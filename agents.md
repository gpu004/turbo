## Commands

- Install: `npm install`
- Types: `npm run types`
- Test: `npm test`
- Check: `npm run check`
- Develop: `npm run dev`
- Deploy: `npm run deploy`

## Architecture

Worker → per-queue Durable Object broker
Broker → one R2 `queue.json` per queue, updated with etag CAS

## Conventions

- No code comments.
- Use conventional commits; the commit-msg hook enforces them.
- Every lint is an error; add a lint rule when a bad pattern recurs.
- Every bug fix gets a regression test.
- After editing `wrangler.jsonc` vars, rerun `npm run types`; `worker-configuration.d.ts` is gitignored.
- Tests run in workerd via `@cloudflare/vitest-plugin`; no Cloudflare account is needed.
- Remote R2's one-write-per-second-per-key limit is not simulated locally.
- `npm run lint` includes the vendored anti-slop plugin.
