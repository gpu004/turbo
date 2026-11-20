import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { Settings } from "../src/config";

describe("Settings", () => {
  it("reads integers in base 10 and falls back on anything else", () => {
    const env = {
      QUEUE_BATCH_WINDOW_MS: "",
      QUEUE_LEASE_SECONDS: "30s",
      MAX_QUEUE_BYTES: "0x800",
      MAX_REQUEST_BYTES: "4096",
      COMPLETED_RETENTION_SECONDS: "60",
    };

    const settings = Effect.runSync(
      Settings.use(Effect.succeed).pipe(Effect.provide(Settings.fromEnv(env))),
    );

    expect(settings).toEqual({
      batchWindowMs: 5,
      leaseSeconds: 30,
      maxQueueBytes: 8 * 1024 * 1024,
      maxRequestBytes: 4096,
      completedRetentionSeconds: 60,
    });
  });
});
