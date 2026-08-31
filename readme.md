# turbo

`turbo` is a small distributed job queue built around one JSON object per queue in Cloudflare R2.

Clients talk to a Worker. The Worker routes every operation for a named queue to the same Durable Object broker. The broker batches concurrent pushes, claims, heartbeats, and completions into one conditional R2 write. It replies only after R2 accepts that write.

R2 is the durable record. Durable Objects coordinate access, but the broker can restart and reconstruct the queue from `queues/<name>/queue.json`.

## What it guarantees

- FIFO claims among jobs that are ready to run
- at-least-once execution through expiring leases
- fencing tokens, so an old worker cannot heartbeat or complete a reassigned job
- idempotent pushes when the client supplies an `Idempotency-Key`
- conditional R2 writes, so an unexpected second writer cannot silently overwrite the queue
- one broker per queue, which avoids a global Durable Object bottleneck

The JSON-file design rewrites the whole queue on each group commit. It is meant for queues whose metadata stays comfortably below the configured 8 MiB default. Job payloads should contain references to large data, not the data itself. The lower cap leaves room for the source object, the updated copy, and serialized JSON inside Cloudflare's 128 MB isolate limit.

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

`npm run lint` uses Oxlint. `npm run format` writes Oxfmt formatting, and `npm run format:check` verifies it without changing files. `npm run check` runs linting, formatting checks, the Wrangler dry run, and TypeScript checking.

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

The claim response contains a `leaseToken` for each job. Send it with heartbeats and completion calls:

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

Put the API behind Cloudflare Access or a private service binding before using it with production jobs. This repository does not ship a shared secret in source or Wrangler configuration.

## Configuration

The defaults live in `wrangler.jsonc`:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `QUEUE_BATCH_WINDOW_MS` | `5` | Time the broker waits to collect the first commit batch |
| `QUEUE_LEASE_SECONDS` | `60` | Time before another worker may reclaim a job |
| `MAX_QUEUE_BYTES` | `8388608` | Maximum serialized queue object size |
| `MAX_REQUEST_BYTES` | `262144` | Maximum JSON request body size |

Each named queue uses its own Durable Object and R2 key. Split a hot logical workload across queue names when one broker reaches its request-rate limit.
