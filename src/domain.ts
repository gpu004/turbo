import { Clock, Context, Effect, Layer, Schema } from "effect";

// Every identifier is a distinct branded string. The schema below each type is
// the only way to construct one, so a `JobId` cannot be passed where a
// `LeaseToken` is expected, and an unvalidated string cannot be passed at all.

export const QueueName = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/, {
    message: "queue name must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
  }),
).pipe(Schema.brand("QueueName"));

export type QueueName = typeof QueueName.Type;

export const JobId = Schema.NonEmptyString.pipe(Schema.brand("JobId"));

export type JobId = typeof JobId.Type;

export const LeaseToken = Schema.NonEmptyString.pipe(Schema.brand("LeaseToken"));

export type LeaseToken = typeof LeaseToken.Type;

export const CommitId = Schema.NonEmptyString.pipe(Schema.brand("CommitId"));

export type CommitId = typeof CommitId.Type;

export const WorkerId = Schema.NonEmptyString.pipe(Schema.brand("WorkerId"));

export type WorkerId = typeof WorkerId.Type;

export const IdempotencyKey = Schema.NonEmptyString.check(
  Schema.isMaxLength(200, { message: "idempotency key is too long" }),
).pipe(Schema.brand("IdempotencyKey"));

export type IdempotencyKey = typeof IdempotencyKey.Type;

// This module owns the clock and randomness. Lint forbids `Date`,
// `crypto.randomUUID`, and `Math.random` everywhere else. Time reaches the rest
// of the code through Effect's `Clock` and ids through the `Ids` service, so a
// test can replace either one.

export class Ids extends Context.Service<
  Ids,
  {
    readonly newJobId: () => JobId;
    readonly newLeaseToken: () => LeaseToken;
    readonly newCommitId: () => CommitId;
  }
>()("turbo/Ids") {
  static readonly layer = Layer.succeed(Ids, {
    newJobId: () => JobId.make(crypto.randomUUID()),
    newLeaseToken: () => LeaseToken.make(crypto.randomUUID()),
    newCommitId: () => CommitId.make(crypto.randomUUID()),
  });
}

// Instants stay ISO-8601 strings so the stored R2 format is unchanged. All
// arithmetic and comparison on them lives in this module.

export const Instant = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?Z$/, {
    message: "expected an ISO-8601 UTC datetime",
  }),
  Schema.makeFilter((value) => Number.isFinite(Date.parse(value)), {
    message: "expected an ISO-8601 UTC datetime",
  }),
).pipe(Schema.brand("Instant"));

export type Instant = typeof Instant.Type;

export const currentInstant: Effect.Effect<Instant> = Clock.currentTimeMillis.pipe(
  Effect.map((millis) => Instant.make(new Date(millis).toISOString())),
);

export function addSeconds(at: Instant, seconds: number): Instant {
  return Instant.make(new Date(Date.parse(at) + seconds * 1_000).toISOString());
}

export function isAtOrAfter(at: Instant, reference: Instant): boolean {
  return Date.parse(at) >= Date.parse(reference);
}

export type JsonValue = Schema.Json;

export const JsonValue = Schema.Json;

// Untrusted JSON text is decoded only through this schema, never `JSON.parse`
// (lint enforces this). Callers still decode the value with a schema for the
// shape they expect before trusting its contents.
export const JsonText = Schema.fromJsonString(JsonValue);
