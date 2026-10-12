import { describe, expect, it } from "vitest";
import {
  applyBatch,
  emptyQueue,
  normalizeQueueFile,
  publicJob,
  publicQueueFile,
  type QueueMutation,
} from "../src/model";

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
        retainSeconds: 3_600,
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

    const completed = applyBatch(heartbeat.file, [complete("job-1", "lease-a", heartbeatAt)]);
    expect(completed.file.jobs).toEqual([]);
    expect(completed.results[0]).toMatchObject({
      ok: true,
      kind: "complete",
      alreadyCompleted: false,
    });
  });

  it("keeps completed idempotency keys as tombstones", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1", "wal-1")]).file;
    const leased = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]).file;
    const completed = applyBatch(leased, [complete("job-1", "lease-a", NOW)]);
    const duplicate = applyBatch(completed.file, [
      push("job-2", "wal-1", "2026-08-31T12:00:01.000Z"),
    ]);

    expect(duplicate.results[0]).toEqual({
      ok: true,
      kind: "push",
      duplicate: true,
      jobId: "job-1",
      completedAt: NOW,
    });
    expect(duplicate.file.jobs).toEqual([]);
    expect(duplicate.file.completed).toHaveLength(1);
  });

  it("prunes expired tombstones and allows the idempotency key again", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1", "wal-1")]).file;
    const leased = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]).file;
    const completed = applyBatch(leased, [complete("job-1", "lease-a", NOW, 1)]);
    const afterExpiry = applyBatch(completed.file, [
      push("job-2", "wal-1", "2026-08-31T12:00:01.000Z"),
    ]);

    expect(afterExpiry.results[0]).toMatchObject({
      ok: true,
      kind: "push",
      duplicate: false,
      job: { id: "job-2" },
    });
    expect(afterExpiry.file.jobs).toHaveLength(1);
    expect(afterExpiry.file.completed).toEqual([]);
  });

  it("distinguishes unknown jobs and repeated completed jobs", () => {
    const unknown = applyBatch(emptyQueue("indexing", NOW), [complete("missing", "lease-a", NOW)]);
    expect(unknown.results[0]).toMatchObject({
      ok: false,
      code: "job_not_found",
      message: "job does not exist",
    });

    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1")]).file;
    const leased = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]).file;
    const completed = applyBatch(leased, [complete("job-1", "lease-a", NOW)]);
    const repeated = applyBatch(completed.file, [complete("job-1", "lease-a", NOW)]);
    const mismatched = applyBatch(completed.file, [complete("job-1", "lease-b", NOW)]);

    expect(repeated.results[0]).toMatchObject({
      ok: true,
      kind: "complete",
      alreadyCompleted: true,
    });
    expect(mismatched.results[0]).toMatchObject({
      ok: false,
      kind: "complete",
      code: "lease_mismatch",
    });
  });

  it("redacts lease tokens from public jobs and queue files", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1", "wal-1")]).file;
    const leased = applyBatch(queued, [claim("worker-a", ["lease-a"], NOW)]);
    const claimResult = leased.results[0];
    if (!claimResult || claimResult.kind !== "claim" || !claimResult.jobs[0]) {
      throw new Error("expected claimed job");
    }
    const publicClaim = publicJob(claimResult.jobs[0]);
    const publicLeasedFile = publicQueueFile(leased.file);
    const completed = applyBatch(leased.file, [complete("job-1", "lease-a", NOW)]);
    const publicCompletedFile = publicQueueFile(completed.file);

    expect(publicClaim).not.toHaveProperty("lease.token");
    expect(publicLeasedFile.jobs[0]?.lease).not.toHaveProperty("token");
    expect(JSON.stringify(publicCompletedFile)).not.toContain('"leaseToken"');
  });

  it("rejects selected pushes without discarding other mutations", () => {
    const queued = applyBatch(emptyQueue("indexing", NOW), [push("job-1")]).file;
    const applied = applyBatch(
      queued,
      [claim("worker-a", ["lease-a"], NOW), push("job-2")],
      new Set([1]),
    );

    expect(applied.results[1]).toMatchObject({
      ok: false,
      kind: "push",
      code: "queue_full",
    });
    expect(applied.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1" }],
    });
    expect(applied.file.jobs).toHaveLength(1);
    expect(applied.file.jobs[0]?.state).toBe("leased");
  });

  it("normalizes legacy queue files without completed tombstones", () => {
    const legacy = {
      version: 1,
      queue: "indexing",
      broker: { kind: "durable-object", name: "indexing" },
      revision: 0,
      updatedAt: NOW,
      jobs: [],
    };

    expect(normalizeQueueFile(legacy, "indexing").completed).toEqual([]);
  });
});

function push(id: string, idempotencyKey?: string, now = NOW): QueueMutation {
  return {
    kind: "push",
    id,
    payload: { id },
    ...(idempotencyKey ? { idempotencyKey } : {}),
    now,
  };
}

function complete(
  jobId: string,
  leaseToken: string,
  now = NOW,
  retainSeconds = 3_600,
): QueueMutation {
  return { kind: "complete", jobId, leaseToken, now, retainSeconds };
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
