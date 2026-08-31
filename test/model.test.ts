import { describe, expect, it } from "vitest";
import { applyBatch, emptyQueue, type QueueMutation } from "../src/model";

const NOW = "2026-08-31T12:00:00.000Z";

describe("queue model", () => {
  it("deduplicates pushes by idempotency key", () => {
    const pushed = applyBatch(emptyQueue("indexing", NOW), [
      push("job-1", "wal-1"),
      push("job-2", "wal-1"),
    ]);

    expect(pushed.file.jobs).toHaveLength(1);
    expect(pushed.results[1]).toMatchObject({ ok: true, kind: "push", duplicate: true });
    expect(pushed.file.revision).toBe(1);
  });

  it("claims in FIFO order and fences a reassigned lease", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1"), push("job-2")]).file;
    const firstClaim = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]);
    expect(firstClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 1 }],
    });

    const afterExpiry = "2026-08-31T12:01:01.000Z";
    const secondClaim = applyBatch(firstClaim.file, [claim("worker-b", ["lease-b"], afterExpiry)]);
    expect(secondClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 2 }],
    });

    const oldCompletion = applyBatch(secondClaim.file, [
      {
        kind: "complete",
        jobId: "job-1",
        leaseToken: "lease-a",
        now: afterExpiry,
      },
    ]);
    expect(oldCompletion.results[0]).toMatchObject({ ok: false, code: "lease_mismatch" });
    expect(oldCompletion.file.jobs).toHaveLength(2);
  });

  it("extends a live lease and removes a completed job", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1")]).file;
    const leased = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]).file;
    const heartbeatAt = "2026-08-31T12:00:30.000Z";
    const heartbeat = applyBatch(leased, [
      {
        kind: "heartbeat",
        jobId: "job-1",
        leaseToken: "lease-a",
        leaseSeconds: 60,
        now: heartbeatAt,
      },
    ]);
    expect(heartbeat.results[0]).toMatchObject({
      ok: true,
      kind: "heartbeat",
      job: { lease: { expiresAt: "2026-08-31T12:01:30.000Z" } },
    });

    const completed = applyBatch(heartbeat.file, [
      {
        kind: "complete",
        jobId: "job-1",
        leaseToken: "lease-a",
        now: heartbeatAt,
      },
    ]);
    expect(completed.file.jobs).toEqual([]);
    expect(completed.results[0]).toMatchObject({
      ok: true,
      kind: "complete",
      alreadyCompleted: false,
    });
  });
});

function push(id: string, idempotencyKey?: string): QueueMutation {
  return {
    kind: "push",
    id,
    payload: { id },
    ...(idempotencyKey ? { idempotencyKey } : {}),
    now: NOW,
  };
}

function claim(workerId: string, leaseTokens: string[], now: string): QueueMutation {
  return {
    kind: "claim",
    workerId,
    maxJobs: leaseTokens.length,
    leaseTokens,
    leaseSeconds: 60,
    now,
  };
}
