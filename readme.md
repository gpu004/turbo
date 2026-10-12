# turbo

`turbo` is a distributed job queue that stores each queue as one JSON object in Cloudflare R2.

Clients talk to a Worker. The Worker routes every operation for a named queue to the same Durable Object broker. The broker batches concurrent pushes, claims, heartbeats, and completions into one conditional R2 write. It replies only after R2 accepts that write.

R2 is the durable record. Durable Objects coordinate access, but the broker can restart and reconstruct the queue from `queues/<name>/queue.json`.

## What it guarantees

- FIFO claims among jobs that are ready to run
- at-least-once execution through expiring leases
- fencing tokens, so an old worker cannot heartbeat or complete a reassigned job
- idempotent pushes when the client supplies an `Idempotency-Key`
- conditional R2 writes, so an unexpected second writer cannot silently overwrite the queue
- one broker per queue, which avoids a global Durable Object bottleneck

Each group commit rewrites the whole queue. Keep queue metadata below the default 8 MiB cap. Job payloads should reference large data rather than contain it. The cap leaves room for the source object, the updated copy, and serialized JSON within Cloudflare's 128 MB isolate limit.

## Run it locally

```sh
npm install
npm run types
npm test
npm run lint
npm run format:check
npm run dev
```

Wrangler simulates both R2 and Durable Objects locally.

`npm run lint` uses Oxlint with the vendored anti-slop plugin at `tools/oxlint/anti-slop`. See the [upstream record](tools/oxlint/anti-slop/UPSTREAM.md) for its source and local changes.

`npm run format` formats files with Oxfmt. `npm run format:check` checks formatting without changing files. `npm run verify` runs linting, formatting checks, `bun check` for types, `bun audit` for dependency vulnerabilities, and a Wrangler dry run. The typecheck and audit scripts need Bun.

CI runs `npm run verify` and `npm test` on pull requests, on pushes to main, and daily.

## API

Create a job:

```sh
curl -i http://localhost:8787/queues/indexing/jobs \
  -H 'content-type: application/json' \
  -H 'idempotency-key: namespace-42/wal-9001' \
  --data '{"payload":{"namespace":"42","walSequence":9001}}'
```

Claim up to ten jobs:

```sh
curl -s http://localhost:8787/queues/indexing/claims \
  -H 'content-type: application/json' \
  --data '{"workerId":"indexer-7","maxJobs":10}'
```

Each job in the claim response contains a token at `lease.token`. Send that token as `leaseToken` with heartbeats and completion calls:

```sh
curl -s http://localhost:8787/queues/indexing/jobs/JOB_ID/heartbeat \
  -H 'content-type: application/json' \
  --data '{"leaseToken":"LEASE_TOKEN"}'

curl -s http://localhost:8787/queues/indexing/jobs/JOB_ID/complete \
  -H 'content-type: application/json' \
  --data '{"leaseToken":"LEASE_TOKEN"}'
```

Inspect the durable queue state:

```sh
curl -s http://localhost:8787/queues/indexing
```

`GET /health` does not touch R2. It only confirms that the Worker is running.

## Deploy

Create the bucket once, then deploy:

```sh
npx wrangler r2 bucket create turbo-queues
npm run deploy
```

Put the API behind Cloudflare Access or a private service binding before using it with production jobs. Neither the source nor the Wrangler configuration includes a shared secret.

## Configuration

The defaults live in `wrangler.jsonc`:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `QUEUE_BATCH_WINDOW_MS` | `5` | Time the broker waits to collect the first commit batch |
| `QUEUE_LEASE_SECONDS` | `60` | Time before another worker may reclaim a job |
| `MAX_QUEUE_BYTES` | `8388608` | Maximum serialized queue object size |
| `MAX_REQUEST_BYTES` | `262144` | Maximum JSON request body size |

Each named queue uses its own Durable Object and R2 key. Split work across queue names when one broker reaches its request-rate limit.

## License

[MIT](LICENSE).
