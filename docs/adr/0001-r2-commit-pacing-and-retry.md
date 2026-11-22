# ADR 0001: R2 Commit Pacing, Retryable Errors, and Ambiguous Write Recovery

## Context

Each named queue is stored as a single JSON object in Cloudflare R2 (`queues/<name>/queue.json`) and updated via conditional compare-and-set `put` calls from a Durable Object broker (`src/broker.ts`). Cloudflare R2 enforces roughly one write per second per object key, surfaces error codes only as a trailing `(code)` suffix on error messages in Workers bindings, and can apply a write while returning an ambiguous network or server failure to the caller.

## Decisions

### 1. Pace writes by `QUEUE_COMMIT_INTERVAL_MS` from write start

`waitForTurn` and `pacedPut` enforce a minimum interval of `QUEUE_COMMIT_INTERVAL_MS` (default `1000` ms) between the starts of consecutive R2 `put` calls for the same queue.

### 2. Wait before draining batches and before applying commit retries

- `flushLoop` calls `waitForTurn` before draining `pending` so commands arriving during the pacing cooldown coalesce into the next commit batch rather than triggering an extra write.
- `commitOnce` calls `waitForTurn` before loading and applying the batch so a retry does not stamp leases with a timestamp that a subsequent pacing wait inside `pacedPut` would eat into.
- `PendingCommand.apply` receives `now` at commit-attempt time rather than request-arrival time because a command may wait a commit interval or more before committing, and a lease must start when it is written.

### 3. Enforce a minimum lease of two commit intervals

`src/config.ts` clamps `leaseSeconds` to at least `Math.ceil((2 * commitIntervalMs) / 1_000)`. A heartbeat or completion sent immediately after a claim can wait up to one commit interval for its write; a lease of at least two intervals outlives that wait while leaving at least one interval for the worker.

### 4. Retry specific R2 numeric error codes with backoff

Cloudflare R2 ends error messages with its numeric code (`https://developers.cloudflare.com/r2/api/error-codes/`). `RETRYABLE_R2_CODE` matches the codes where retrying the same request may succeed:

- `10058`: too many requests (per-key write rate limit)
- `10001`: internal error
- `10043`: service unavailable
- `10054`: client disconnected

### 5. Recover from ambiguous R2 write failures with `commitId`

An R2 write that fails with an error may still have landed. Each commit generates a `commitId`, writes it into `QueueFile.commitId`, and stores the batch's delivery effects in an `unconfirmed` map keyed by `commitId` when `pacedPut` fails. After clearing the cached snapshot and reloading from R2, if the reloaded `QueueFile.commitId` matches an entry in `unconfirmed`, the broker delivers those recorded results instead of applying the batch a second time.

### 6. Derive HTTP `Retry-After` from `QUEUE_COMMIT_INTERVAL_MS`

`unavailableResponse` in `src/index.ts` returns `503` with `Retry-After: Math.max(1, Math.ceil(commitIntervalMs / 1_000))` because the broker writes at most once per `QUEUE_COMMIT_INTERVAL_MS`, which is the earliest a retry can land in a new commit.

## Consequences

- Bursts coalesce cleanly into ~1 R2 write per second instead of failing with `429`/`500`.
- Lease timing stays accurate across pacing delays and retries.
- Ambiguous write failures avoid double-application provided no other writer overwrote the key in between.
- `spec/QueueBroker.tla` must stay aligned with any changes to `src/broker.ts` loading, caching, or commit retry logic.
