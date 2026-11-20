import { z } from "zod";

// Env vars are parsed once into typed settings. Code outside this module reads
// `Config`, never `env.*` strings. An out-of-range value falls back to its
// default, matching the documented behaviour of each variable.

function boundedInteger(fallback: number, minimum: number, maximum: number) {
  return z.coerce.number().int().min(minimum).max(maximum).catch(fallback);
}

const ConfigSchema = z
  .object({
    QUEUE_BATCH_WINDOW_MS: boundedInteger(5, 0, 1_000),
    QUEUE_LEASE_SECONDS: boundedInteger(60, 1, 86_400),
    MAX_QUEUE_BYTES: boundedInteger(8 * 1024 * 1024, 1_024, 32 * 1024 * 1024),
    MAX_REQUEST_BYTES: boundedInteger(256 * 1024, 1_024, 4 * 1024 * 1024),
  })
  .transform((vars) => ({
    batchWindowMs: vars.QUEUE_BATCH_WINDOW_MS,
    leaseSeconds: vars.QUEUE_LEASE_SECONDS,
    maxQueueBytes: vars.MAX_QUEUE_BYTES,
    maxRequestBytes: vars.MAX_REQUEST_BYTES,
  }));

export type Config = Readonly<z.infer<typeof ConfigSchema>>;

export function parseConfig(env: Env): Config {
  return ConfigSchema.parse(env);
}
