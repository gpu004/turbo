import { describe, expect, expectTypeOf, it } from "vitest";
import { IdempotencyKey, Instant, JobId, LeaseToken, QueueName, WorkerId } from "../src/domain";
import {
  applyBatch,
  closeDraft,
  emptyQueue,
  invariantViolation,
  openDraft,
  parseQueueFile,
  type LeasedJob,
  type PendingJob,
  type QueueMutation,
} from "../src/model";

const NOW = Instant.parse("2026-08-31T12:00:00.000Z");

const Q = QueueName.parse("indexing");

describe("queue file parser", () => {
  it("parses a valid queue file", () => {
    const file = emptyQueue(Q, NOW);

    expect(parseQueueFile(JSON.stringify(file), Q)).toEqual(file);
  });

  it("rejects a queue file for a different queue", () => {
    const file = emptyQueue(QueueName.parse("other"), NOW);

    expect(() => parseQueueFile(JSON.stringify(file), Q)).toThrow(
      "R2 object for queue indexing has an incompatible format",
    );
  });

  it("rejects invalid JSON and malformed queue files", () => {
    expect(() => parseQueueFile("not json", Q)).toThrow(
      "R2 object for queue indexing is malformed",
    );
    expect(() => parseQueueFile('{"version":1,"queue":"indexing","jobs":"x"}', Q)).toThrow(
      "R2 object for queue indexing is malformed",
    );
  });

  it("rejects a leased job without a lease", () => {
    const file = {
      ...emptyQueue(Q, NOW),
      jobs: [{ id: "j", payload: null, enqueuedAt: NOW, attempts: 1, state: "leased" }],
    };

    expect(() => parseQueueFile(JSON.stringify(file), Q)).toThrow("is malformed");
  });

  it("rejects a stored queue that breaks an invariant", () => {
    const job = { id: "j", payload: null, enqueuedAt: NOW, attempts: 0, state: "pending" };
    const file = { ...emptyQueue(Q, NOW), jobs: [job, job] };

    expect(() => parseQueueFile(JSON.stringify(file), Q)).toThrow("is malformed");
  });
});

describe("queue invariants", () => {
  it("refuses to close a draft that breaks one", () => {
    const draft = openDraft(
      applyBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file,
      tokens(),
    );

    const [job] = draft.jobs;

    if (!job) throw new Error("expected one job");
    draft.jobs.push(job);
    draft.changedAt = NOW;

    expect(() => closeDraft(draft)).toThrow("job job-1 appears twice");
  });

  it("reports each kind of violation", () => {
    const pending: PendingJob = {
      id: JobId.parse("a"),
      payload: null,
      enqueuedAt: NOW,
      attempts: 0,
      state: "pending",
    };

    const leased: LeasedJob = {
      ...pending,
      id: JobId.parse("b"),
      state: "leased",
      attempts: 1,
      lease: {
        token: LeaseToken.parse("t"),
        workerId: WorkerId.parse("w"),
        heartbeatAt: NOW,
        expiresAt: Instant.parse("2026-08-31T12:01:00.000Z"),
      },
    };

    const key = IdempotencyKey.parse("k");

    expect(invariantViolation([pending, leased])).toBeUndefined();
    expect(invariantViolation([pending, pending])).toMatch("appears twice");
    expect(
      invariantViolation([
        { ...pending, idempotencyKey: key },
        { ...leased, idempotencyKey: key },
      ]),
    ).toMatch("used by two jobs");
    expect(invariantViolation([{ ...pending, attempts: 1 }])).toMatch("has 1 attempts");
    expect(invariantViolation([{ ...leased, attempts: 0 }])).toMatch("has no attempts");
    expect(invariantViolation([leased, { ...leased, id: JobId.parse("c") }])).toMatch(
      "lease token",
    );
    expect(invariantViolation([{ ...leased, lease: { ...leased.lease, expiresAt: NOW } }])).toMatch(
      "expires before its last heartbeat",
    );
  });
});

describe("queue model", () => {
  it("deduplicates pushes by idempotency key", () => {
    const pushed = applyBatch(
      emptyQueue(Q, NOW),
      [push("job-1", "wal-1"), push("job-2", "wal-1")],
      tokens(),
    );

    expect(pushed.file.jobs).toHaveLength(1);
    expect(pushed.results[1]).toMatchObject({ ok: true, kind: "push", duplicate: true });
    expect(pushed.file.revision).toBe(1);
  });

  it("claims in FIFO order and fences a reassigned lease", () => {
    const queued = applyBatch(emptyQueue(Q, NOW), [push("job-1"), push("job-2")], tokens()).file;
    const firstClaim = applyBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a"));

    expect(firstClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 1 }],
    });

    const afterExpiry = Instant.parse("2026-08-31T12:01:01.000Z");

    const secondClaim = applyBatch(
      firstClaim.file,
      [claim("worker-b", 1, afterExpiry)],
      tokens("lease-b"),
    );

    expect(secondClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 2 }],
    });

    const oldCompletion = applyBatch(
      secondClaim.file,
      [complete("job-1", "lease-a", afterExpiry)],
      tokens(),
    );

    expect(oldCompletion.results[0]).toMatchObject({ ok: false, code: "lease_mismatch" });
    expect(oldCompletion.file.jobs).toHaveLength(2);
  });

  it("extends a live lease and removes a completed job", () => {
    const queued = applyBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const leased = applyBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a")).file;
    const heartbeatAt = Instant.parse("2026-08-31T12:00:30.000Z");

    const heartbeat = applyBatch(
      leased,
      [
        {
          kind: "heartbeat",
          jobId: JobId.parse("job-1"),
          leaseToken: LeaseToken.parse("lease-a"),
          leaseSeconds: 60,
          now: heartbeatAt,
        },
      ],
      tokens(),
    );

    expect(heartbeat.results[0]).toMatchObject({
      ok: true,
      kind: "heartbeat",
      job: { lease: { expiresAt: "2026-08-31T12:01:30.000Z" } },
    });

    const completed = applyBatch(
      heartbeat.file,
      [complete("job-1", "lease-a", heartbeatAt)],
      tokens(),
    );

    expect(completed.file.jobs).toEqual([]);
    expect(completed.results[0]).toMatchObject({
      ok: true,
      kind: "complete",
      alreadyCompleted: false,
    });
  });

  it("does not change a result already handed out when a later mutation in the batch touches the job", () => {
    const queued = applyBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const later = Instant.parse("2026-08-31T12:00:30.000Z");

    const batch = applyBatch(
      queued,
      [
        claim("worker-a", 1, NOW),
        {
          kind: "heartbeat",
          jobId: JobId.parse("job-1"),
          leaseToken: LeaseToken.parse("lease-a"),
          leaseSeconds: 60,
          now: later,
        },
      ],
      tokens("lease-a"),
    );

    expect(batch.results[0]).toMatchObject({
      jobs: [{ lease: { heartbeatAt: NOW } }],
    });

    expect(batch.file.revision).toBe(queued.revision + 1);
  });

  it("leaves the source file untouched", () => {
    const source = applyBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const before = structuredClone(source);
    applyBatch(source, [claim("worker-a", 1, NOW)], tokens("lease-a"));

    expect(source).toEqual(before);
  });
});

describe("types", () => {
  it("keeps identifiers apart", () => {
    expectTypeOf<JobId>().not.toEqualTypeOf<LeaseToken>();
    expectTypeOf<string>().not.toExtend<JobId>();
  });

  it("ties a lease to the leased state", () => {
    expectTypeOf<PendingJob>().not.toHaveProperty("lease");
    expectTypeOf<LeasedJob["lease"]>().not.toBeNullable();
  });
});

function push(id: string, idempotencyKey?: string): QueueMutation {
  return {
    kind: "push",
    id: JobId.parse(id),
    idempotencyKey: idempotencyKey === undefined ? undefined : IdempotencyKey.parse(idempotencyKey),
    payload: { id },
    now: NOW,
  };
}

function claim(workerId: string, maxJobs: number, now: Instant): QueueMutation {
  return { kind: "claim", workerId: WorkerId.parse(workerId), maxJobs, leaseSeconds: 60, now };
}

function complete(jobId: string, leaseToken: string, now: Instant): QueueMutation {
  return {
    kind: "complete",
    jobId: JobId.parse(jobId),
    leaseToken: LeaseToken.parse(leaseToken),
    now,
  };
}

// Hands out the given lease tokens in order, and fails the test if the model
// asks for more than it was given.
function tokens(...values: string[]): () => LeaseToken {
  const queue = values.map((value) => LeaseToken.parse(value));

  return () => {
    const next = queue.shift();

    if (!next) throw new Error("model requested more lease tokens than the test provided");

    return next;
  };
}
