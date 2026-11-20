import { describe, expect, it } from "vitest";
import { parseConfig } from "../src/config";

describe("parseConfig", () => {
  it("reads integers in base 10 and falls back on anything else", () => {
    const env = {
      QUEUE_BATCH_WINDOW_MS: "",
      QUEUE_LEASE_SECONDS: "30s",
      MAX_QUEUE_BYTES: "0x800",
      MAX_REQUEST_BYTES: "4096",
    };

    expect(parseConfig(env)).toEqual({
      batchWindowMs: 5,
      leaseSeconds: 30,
      maxQueueBytes: 8 * 1024 * 1024,
      maxRequestBytes: 4096,
    });
  });
});
