# ADR 0004: Worker-to-Durable-Object RPC Boundary and Type-Level Lease Redaction

## Context

The HTTP Worker (`src/index.ts`) calls methods on `QueueBroker` (`src/broker.ts`) over Cloudflare Workers Durable Object RPC, and returns job state across both public (`GET /queues/:queue`, duplicate `POST /queues/:queue/jobs`) and lease-holder (`POST /queues/:queue/claims`, `POST /queues/:queue/jobs/:jobId/heartbeat`) endpoints.

## Decisions

### 1. Carry RPC payloads as JSON text tagged with phantom `Encoded<T>`

Cloudflare's Durable Object RPC stub types cannot map the recursive `JsonValue` type. `src/rpc.ts` serializes values to JSON text on send (`encode<T>`) and parses them on receipt (`decode<T>`), attaching a phantom unique-symbol brand `Encoded<T>` so the caller recovers the exact type `T` sent across the boundary without re-validating trusted internal data.

### 2. Return retryable broker failures as `Unavailable` values

Errors thrown across the Durable Object RPC boundary lose their custom tagged types. `QueueBroker.mutate` catches `Overloaded` and `CommitAttemptsExhausted` — the two errors a client should retry later — and returns them as a typed `Unavailable` result (`{ ok: false, kind: "unavailable", code, message }`), which the Worker translates into `503` with `Retry-After`.

### 3. Enforce lease token redaction at the type level

`PublicLease` declares `readonly token?: never` and `PublicCompletedJob` declares `readonly leaseToken?: never` in `src/model.ts`. In TypeScript's structural type system, omitting a property alone would still allow assigning a `Lease` to a `PublicLease`; the `?: never` field makes accidentally returning a `LeasedJob` or `CompletedJob` to a non-holder caller a compile-time type error (verified in `test/types.test.ts`).

## Consequences

- All `JSON.parse` calls for trusted internal RPC traffic stay isolated in `src/rpc.ts` (enforced by `turbo/no-json-parse`).
- Clients receive structured `503` responses with `Retry-After` for overload and exhausted storage retries, while unexpected defects still surface as logged `500`s.
- Lease tokens cannot leak through public response types without failing `tsc --noEmit`.
