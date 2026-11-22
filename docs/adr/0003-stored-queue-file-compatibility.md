# ADR 0003: Stored `QueueFile` Format and Cross-Version Compatibility

## Context

`QueueFile` (`src/model.ts`) is persisted in R2 and read/rewritten by live Durable Object brokers. During rolling deployments or rollbacks, older and newer broker versions can take turns reading and rewriting the same R2 object.

## Decisions

### 1. Represent `Instant` as ISO-8601 UTC strings

`Instant` in `src/domain.ts` stays a branded ISO-8601 UTC string rather than an epoch millisecond number so the stored R2 `QueueFile` format remains unchanged. All parsing, arithmetic, and comparison on instants live in `src/domain.ts`.

### 2. Preserve unknown fields with `storedStruct`

Every object schema inside `QueueFileSchema` is built with `storedStruct` (`Schema.StructWithRest(..., [Schema.Record(Schema.String, JsonValue)])`). When an older broker loads and rewrites a queue, fields added by a newer broker survive intact.

### 3. Keep additive fields optional and omit empty `completed`

- `QueueFile.commitId` and `QueueFile.completed` are optional so files written by older brokers decode cleanly.
- `closeDraft` omits the `completed` property when the list is empty so a queue that has never completed a job keeps the exact file shape older brokers wrote.

### 4. Resolve mixed-version idempotency key collisions with `supersede`

A broker older than the `completed` field preserves `completed` on rewrite (via `storedStruct`), but does not consult it when accepting pushes — so it can enqueue a new job that reuses a completed job's `idempotencyKey`. `supersede` filters out any completed entry whose `idempotencyKey` is currently held by a queued job: the queued job wins, the file remains loadable rather than failing invariant checks, and the next commit drops the stale completion.

### 5. Decode identity before full schema validation

`parseQueueFile` decodes `QueueIdentitySchema` (`version: 1`, `queue: string`) before `QueueFileSchema` so an object written for a different queue name or an unsupported format version is reported as `IncompatibleQueueFile` rather than `MalformedQueueFile`.

## Consequences

- Adding an optional field to `QueueFile` v1 is safe across rolling deploys and rollbacks.
- Renaming, removing, or changing the semantics of an existing field still requires incrementing `version` and adding a reader for v1 (see `AGENTS.md`).
