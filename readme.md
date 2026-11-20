# turbo

`turbo` is a distributed job queue that stores each queue as one JSON object in Cloudflare R2.

Clients talk to a Worker. The Worker routes every operation for a named queue to the same Durable Object broker. The broker batches concurrent pushes, claims, heartbeats, and completions into one conditional R2 write. It replies only after R2 accepts that write.

R2 is the durable record. Durable Objects coordinate access, but the broker can restart and reconstruct the queue from `queues/<name>/queue.json`.

## What it guarantees

- FIFO claims among jobs that are ready to run
- at-least-once execution through expiring leases
- fencing tokens, so an old worker cannot heartbeat or complete a reassigned job
- idempotent pushes when the client supplies an `Idempotency-Key`, while the job is queued and for `COMPLETED_RETENTION_SECONDS` after it completes
- lease tokens returned only to the worker that holds the lease
- conditional R2 writes, so an unexpected second writer cannot silently overwrite the queue
- one broker per queue, which avoids a global Durable Object bottleneck

Each group commit rewrites the whole queue. Completed jobs are kept in it until their retention ends, so they count toward `MAX_QUEUE_BYTES`. When a commit would exceed the cap, the broker first drops the oldest completed jobs, which makes idempotency after completion best-effort. If that is not enough, it refuses pushes that do not fit with `507`, in arrival order, and commits the rest of the batch. Keep queue metadata below the default 8 MiB cap. Job payloads should reference large data rather than contain it. The cap leaves room for the source object, the updated copy, and serialized JSON within Cloudflare's 128 MB isolate limit.

R2 accepts about one write per second to the same key, so the broker writes a queue at most once per `QUEUE_COMMIT_INTERVAL_MS`. Requests that arrive while it waits join the next commit. A request can therefore take up to about one interval longer than its own R2 write. The broker retries a commit after a retryable R2 error (`429` and transient server errors) with jittered backoff. Each write carries a commit id, so a write that R2 applied but reported as failed is recognised on reload and not applied twice, provided no other broker wrote the queue in between.

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

The Worker and the broker are written with [Effect](https://effect.website). Schemas validate every request and every stored queue. R2, the clock, and id generation are services, so `test/broker.test.ts` runs the commit protocol against an in-memory store.

`npm run lint` uses Oxlint with the vendored anti-slop plugin at `tools/oxlint/anti-slop`, including its Effect rules. See the [upstream record](tools/oxlint/anti-slop/UPSTREAM.md) for its source and local changes.

The project rules in `tools/oxlint/turbo` each confine one operation, such as reading the clock or parsing JSON, to the module that owns it. `npm run lint:rules` tests them against the fixtures in `tools/oxlint/turbo/fixtures`.

`npm run format` formats files with Oxfmt. `npm run format:check` checks formatting without changing files. `npm run check` runs linting, the lint-rule tests, formatting checks, a Wrangler dry run, and TypeScript checking.

`npm run verify:tla` model-checks the broker's commit protocol in `spec/QueueBroker.tla` with TLC. It needs Java and downloads `tla2tools.jar` into `.cache/`. It also checks that the spec fails when the R2 compare-and-set is removed.

## API

Create a job. The response is `201` with the job, or `200` with `"duplicate": true` when the idempotency key is already in use. For a key whose job is still queued the body has the `job`. For one whose job has completed it has `jobId` and `completedAt` instead. A push that does not fit within `MAX_QUEUE_BYTES` gets `507` with code `queue_full`.

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

Each job in the claim response contains a token at `lease.token`. Only claim and heartbeat responses include it. Send that token as `leaseToken` with heartbeats and completion calls:

```sh
curl -s http://localhost:8787/queues/indexing/jobs/JOB_ID/heartbeat \
  -H 'content-type: application/json' \
  --data '{"leaseToken":"LEASE_TOKEN"}'

curl -s http://localhost:8787/queues/indexing/jobs/JOB_ID/complete \
  -H 'content-type: application/json' \
  --data '{"leaseToken":"LEASE_TOKEN"}'
```

A stale or mismatched lease token gets `409`, and a job that is not queued gets `404` with code `job_not_found`. Completing a job again with the token that completed it returns `200` with `"alreadyCompleted": true` while the completion is retained, and `404` after that.

Inspect the durable queue state. Lease tokens are removed from the response, and `completed` lists the retained completions:

```sh
curl -s http://localhost:8787/queues/indexing
```

`GET /health` does not touch R2. It only confirms that the Worker is running.

Any mutation can get `503` with a `Retry-After` header. The code is `overloaded` when `MAX_PENDING_MUTATIONS` requests, or `MAX_QUEUE_BYTES` of pushed payloads, are already waiting for the next commit, and `storage_unavailable` when R2 still fails after five attempts. A `storage_unavailable` mutation may still have been committed, so retry pushes with the same idempotency key.

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
| `QUEUE_COMMIT_INTERVAL_MS` | `1000` | Minimum time between the starts of two R2 writes for one queue |
| `MAX_PENDING_MUTATIONS` | `10000` | Requests waiting for a commit before new ones get `503` |
| `COMPLETED_RETENTION_SECONDS` | `3600` | How long a completed job's idempotency key and lease token are kept |
| `MAX_QUEUE_BYTES` | `8388608` | Maximum serialized queue object size |
| `MAX_REQUEST_BYTES` | `262144` | Maximum JSON request body size |

Each named queue uses its own Durable Object and R2 key. Split work across queue names when one broker reaches its request-rate limit.

## License

[MIT](LICENSE).
