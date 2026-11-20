import { z } from "zod";

// Every identifier is a distinct branded string. The schema below each type is
// the only way to construct one, so a `JobId` cannot be passed where a
// `LeaseToken` is expected, and an unvalidated string cannot be passed at all.

export const QueueName = z
  .string()
  .regex(
    /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/,
    "queue name must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
  )
  .brand<"QueueName">();

export type QueueName = z.infer<typeof QueueName>;

export const JobId = z.string().min(1).brand<"JobId">();

export type JobId = z.infer<typeof JobId>;

export const LeaseToken = z.string().min(1).brand<"LeaseToken">();

export type LeaseToken = z.infer<typeof LeaseToken>;

export function newJobId(): JobId {
  return JobId.parse(crypto.randomUUID());
}

export function newLeaseToken(): LeaseToken {
  return LeaseToken.parse(crypto.randomUUID());
}

export const WorkerId = z.string().min(1).brand<"WorkerId">();

export type WorkerId = z.infer<typeof WorkerId>;

export const IdempotencyKey = z
  .string()
  .min(1)
  .max(200, "idempotency key is too long")
  .brand<"IdempotencyKey">();

export type IdempotencyKey = z.infer<typeof IdempotencyKey>;

// This module owns the clock and randomness. Lint forbids `Date`,
// `crypto.randomUUID`, and `Math.random` everywhere else, so time and ids reach
// the model only as explicit values.

// Instants stay ISO-8601 strings so the stored R2 format is unchanged. All
// arithmetic and comparison on them lives in this module.

export const Instant = z.iso.datetime().brand<"Instant">();

export type Instant = z.infer<typeof Instant>;

export function currentInstant(): Instant {
  return Instant.parse(new Date().toISOString());
}

export function addSeconds(at: Instant, seconds: number): Instant {
  return Instant.parse(new Date(Date.parse(at) + seconds * 1_000).toISOString());
}

export function isAtOrAfter(at: Instant, reference: Instant): boolean {
  return Date.parse(at) >= Date.parse(reference);
}

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export const JsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.null(),
    z.boolean(),
    z.number(),
    z.string(),
    z.array(JsonValueSchema),
    z.record(z.string(), JsonValueSchema),
  ]),
);

export type JsonParse = { ok: true; value: JsonValue } | { ok: false };

// The only place untrusted JSON text is parsed (lint enforces this). Callers
// must still validate the value against a schema before trusting its contents.
export function parseJson(text: string): JsonParse {
  try {
    // SAFETY: `JSON.parse` can only produce null, booleans, numbers, strings,
    // arrays, and plain objects of those, which is exactly `JsonValue`.
    return { ok: true, value: JSON.parse(text) as JsonValue };
  } catch {
    return { ok: false };
  }
}
