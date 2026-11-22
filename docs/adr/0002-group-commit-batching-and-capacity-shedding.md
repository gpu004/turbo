# ADR 0002: Group-Commit Batching, Backpressure, and Capacity Shedding

## Context

`src/broker.ts` batches concurrent `push`, `claim`, `heartbeat`, and `complete` requests into a single conditional R2 write, while running inside Cloudflare Workers' 128 MB isolate memory ceiling and enforcing `MAX_QUEUE_BYTES` and `MAX_REQUEST_BYTES`.

## Decisions

### 1. Type-erased `PendingCommand` closures for heterogeneous batching

Different mutation kinds (`push`, `claim`, `heartbeat`, `complete`) produce different result types, but must share one batch array. `PendingCommand` erases each queued mutation to `apply: (draft, now) => Effect.Effect<void>`: the closure executes `applyMutation` against the draft and returns the effect that resolves its caller's typed `Deferred`, so the typed result never leaves the closure.

### 2. Atomic batch drain and loop termination

In `flushLoop`, draining `pending` and clearing `idle` happen in a single `Ref.modify` step. When `pending` is empty, `idle` becomes `undefined` atomically, guaranteeing that any command enqueued after the last batch always starts a new flush loop instead of stranding without an active worker fiber.

### 3. Generate one `JobId` per `push` across attempts

`Broker.push` allocates `ids.newJobId()` once outside the `(now) => ...` mutation closure so shedding (`refusedPushes: ReadonlySet<JobId>`) and commit retries always refer to the same job identifier.

### 4. Bound in-memory waiting payloads and stream request bodies

- `enqueue` rejects new commands with `Overloaded` (`503`) once `pending.length >= maxPendingMutations` or `pending.length > 0 && pendingBytes + bytes > maxQueueBytes`. Payloads beyond `MAX_QUEUE_BYTES` cannot fit in one commit anyway, and capping them prevents a burst of large pushes from exhausting isolate memory.
- `readJson` in `src/index.ts` reads `request.body` through `Stream.fromReadableStream` and fails as soon as received bytes exceed `MAX_REQUEST_BYTES`; failing the stream cancels the underlying reader so an oversized request body is never buffered to completion.

### 5. Shedding priority under `MAX_QUEUE_BYTES`

When an applied commit exceeds `MAX_QUEUE_BYTES`, `planShedding` in `src/model.ts` sheds state in strict priority order:

1. **Oldest completed jobs first.** `draft.completed` is kept append-only in completion order, so slicing from the front drops the oldest retained completions. Completed entries only record finished work for best-effort idempotency, whereas a queue full of completed records must still accept and lease live work.
2. **New pushes in arrival order, preserving smaller pushes that fit.** Each newly added push is kept while remaining byte room permits it, so a single oversized push is refused with `507 queue_full` without blocking smaller pushes behind it in the same batch.
3. If neither completed jobs nor new pushes remain to shed, `planShedding` returns `undefined` and the commit fails with `QueueTooLarge`.

## Consequences

- Mixed mutation batches stay strongly typed without runtime tags on `Deferred` channels.
- Memory consumption in both the Worker and Durable Object stays bounded under burst traffic.
- Completion idempotency is best-effort under storage pressure, while live queue operations remain available.
