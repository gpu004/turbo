import { Effect } from "effect";
import { describe, expect, expectTypeOf, it } from "vitest";
import { IdempotencyKey, Instant, JobId, LeaseToken, QueueName, WorkerId } from "../src/domain";
import {
  applyBatch,
  applyMutation,
  closeDraft,
  emptyQueue,
  invariantViolation,
  openDraft,
  parseQueueFile,
  type BatchResult,
  type LeasedJob,
  type PendingJob,
  type QueueMutation,
} from "../src/model";

const NOW = Instant.make("2026-08-31T12:00:00.000Z");

const Q = QueueName.make("indexing");

describe("queue file parser", () => {
  it("parses a valid queue file", () => {
    const file = emptyQueue(Q, NOW);

    expect(Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toEqual(file);
  });

  it("rejects a queue file for a different queue", () => {
    const file = emptyQueue(QueueName.make("other"), NOW);

    expect(() => Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toThrow(
      "R2 object for queue indexing has an incompatible format",
    );
  });

  it("rejects invalid JSON and malformed queue files", () => {
    expect(() => Effect.runSync(parseQueueFile("not json", Q))).toThrow(
      "R2 object for queue indexing is malformed",
    );
    expect(() =>
      Effect.runSync(parseQueueFile('{"version":1,"queue":"indexing","jobs":"x"}', Q)),
    ).toThrow("R2 object for queue indexing is malformed");
  });

  it("keeps fields a newer broker added", () => {
    const job = { id: "j", payload: null, enqueuedAt: NOW, attempts: 0, state: "pending", tier: 2 };
    const file = { ...emptyQueue(Q, NOW), shard: "eu", jobs: [job] };

    expect(Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toEqual(file);
  });

  it("rejects a leased job without a lease", () => {
    const file = {
      ...emptyQueue(Q, NOW),
      jobs: [{ id: "j", payload: null, enqueuedAt: NOW, attempts: 1, state: "leased" }],
    };

    expect(() => Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toThrow("is malformed");
  });

  it("rejects a job that is both queued and completed", () => {
    const job = { id: "j", payload: null, enqueuedAt: NOW, attempts: 0, state: "pending" };
    const completed = { jobId: "j", leaseToken: "t", completedAt: NOW, expiresAt: NOW };
    const file = { ...emptyQueue(Q, NOW), jobs: [job], completed: [completed] };

    expect(() => Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toThrow("is malformed");
    expect(
      Effect.runSync(parseQueueFile(JSON.stringify({ ...file, jobs: [] }), Q)).completed,
    ).toEqual([completed]);
  });

  it("rejects a stored queue that breaks an invariant", () => {
    const job = { id: "j", payload: null, enqueuedAt: NOW, attempts: 0, state: "pending" };
    const file = { ...emptyQueue(Q, NOW), jobs: [job, job] };

    expect(() => Effect.runSync(parseQueueFile(JSON.stringify(file), Q))).toThrow("is malformed");
  });
});

describe("queue invariants", () => {
  it("refuses to close a draft that breaks one", () => {
    const draft = openDraft(runBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file, tokens());

    const [job] = draft.jobs;

    if (!job) throw new Error("expected one job");
    draft.jobs.push(job);
    draft.changedAt = NOW;

    expect(() => Effect.runSync(closeDraft(draft))).toThrow("job job-1 appears twice");
  });

  it("reports each kind of violation", () => {
    const pending: PendingJob = {
      id: JobId.make("a"),
      payload: null,
      enqueuedAt: NOW,
      attempts: 0,
      state: "pending",
    };

    const leased: LeasedJob = {
      ...pending,
      id: JobId.make("b"),
      state: "leased",
      attempts: 1,
      lease: {
        token: LeaseToken.make("t"),
        workerId: WorkerId.make("w"),
        heartbeatAt: NOW,
        expiresAt: Instant.make("2026-08-31T12:01:00.000Z"),
      },
    };

    const key = IdempotencyKey.make("k");

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
    expect(invariantViolation([leased, { ...leased, id: JobId.make("c") }])).toMatch("lease token");
    expect(invariantViolation([{ ...leased, lease: { ...leased.lease, expiresAt: NOW } }])).toMatch(
      "expires before its last heartbeat",
    );
  });
});

describe("queue model", () => {
  it("deduplicates pushes by idempotency key", () => {
    const pushed = runBatch(
      emptyQueue(Q, NOW),
      [push("job-1", "wal-1"), push("job-2", "wal-1")],
      tokens(),
    );

    expect(pushed.file.jobs).toHaveLength(1);
    expect(pushed.results[1]).toMatchObject({ ok: true, kind: "push", duplicate: true });
    expect(pushed.file.revision).toBe(1);
  });

  it("claims in FIFO order and fences a reassigned lease", () => {
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1"), push("job-2")], tokens()).file;
    const firstClaim = runBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a"));

    expect(firstClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 1 }],
    });

    const afterExpiry = Instant.make("2026-08-31T12:01:01.000Z");

    const secondClaim = runBatch(
      firstClaim.file,
      [claim("worker-b", 1, afterExpiry)],
      tokens("lease-b"),
    );

    expect(secondClaim.results[0]).toMatchObject({
      ok: true,
      kind: "claim",
      jobs: [{ id: "job-1", attempts: 2 }],
    });

    const oldCompletion = runBatch(
      secondClaim.file,
      [complete("job-1", "lease-a", afterExpiry)],
      tokens(),
    );

    expect(oldCompletion.results[0]).toMatchObject({ ok: false, code: "lease_mismatch" });
    expect(oldCompletion.file.jobs).toHaveLength(2);
  });

  it("extends a live lease and removes a completed job", () => {
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const leased = runBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a")).file;
    const heartbeatAt = Instant.make("2026-08-31T12:00:30.000Z");

    const heartbeat = runBatch(
      leased,
      [
        {
          kind: "heartbeat",
          jobId: JobId.make("job-1"),
          leaseToken: LeaseToken.make("lease-a"),
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

    const completed = runBatch(
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
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const later = Instant.make("2026-08-31T12:00:30.000Z");

    const batch = runBatch(
      queued,
      [
        claim("worker-a", 1, NOW),
        {
          kind: "heartbeat",
          jobId: JobId.make("job-1"),
          leaseToken: LeaseToken.make("lease-a"),
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

  it("keeps a completed job's key until it expires, judged by each mutation's own time", () => {
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1", "wal-1")], tokens()).file;
    const leased = runBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a")).file;
    const done = runBatch(leased, [complete("job-1", "lease-a", NOW)], tokens()).file;

    expect(done.completed).toEqual([
      {
        jobId: "job-1",
        leaseToken: "lease-a",
        idempotencyKey: "wal-1",
        completedAt: NOW,
        expiresAt: "2026-08-31T12:01:00.000Z",
      },
    ]);

    const beforeExpiry = Instant.make("2026-08-31T12:00:59.999Z");
    const afterExpiry = Instant.make("2026-08-31T12:01:00.001Z");

    const batch = runBatch(
      done,
      [
        { ...push("job-2", "wal-1"), now: beforeExpiry },
        { ...push("job-3", "wal-1"), now: afterExpiry },
      ],
      tokens(),
    );

    expect(batch.results).toEqual([
      { ok: true, kind: "push", duplicate: true, jobId: "job-1", completedAt: NOW },
      expect.objectContaining({
        ok: true,
        duplicate: false,
        job: expect.objectContaining({ id: "job-3" }),
      }),
    ]);
    expect(batch.file.completed).toBeUndefined();
  });

  it("answers a repeat completion only for the token that completed the job", () => {
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const leased = runBatch(queued, [claim("worker-a", 1, NOW)], tokens("lease-a")).file;
    const done = runBatch(leased, [complete("job-1", "lease-a", NOW)], tokens()).file;
    const expired = Instant.make("2026-08-31T12:01:00.000Z");

    const repeats = runBatch(
      done,
      [
        complete("job-1", "lease-a", NOW),
        complete("job-1", "lease-b", NOW),
        complete("job-9", "lease-a", NOW),
        complete("job-1", "lease-a", expired),
      ],
      tokens(),
    );

    expect(repeats.results).toMatchObject([
      { ok: true, alreadyCompleted: true },
      { ok: false, code: "lease_mismatch" },
      { ok: false, code: "job_not_found" },
      { ok: false, code: "job_not_found" },
    ]);
    expect(repeats.changed).toBe(false);
  });

  it("refuses the pushes and drops the oldest completed jobs it is told to shed", () => {
    const queued = runBatch(emptyQueue(Q, NOW), [push("job-1"), push("job-2")], tokens()).file;
    const leased = runBatch(queued, [claim("worker-a", 2, NOW)], tokens("a", "b")).file;

    const done = runBatch(
      leased,
      [complete("job-1", "a", NOW), complete("job-2", "b", NOW)],
      tokens(),
    ).file;

    const draft = openDraft(done, tokens(), {
      refusedPushes: new Set([JobId.make("job-3")]),
      evictedCompleted: 1,
    });

    const results = [push("job-3"), push("job-4")].map((mutation) =>
      applyMutation(draft, mutation),
    );

    const closed = Effect.runSync(closeDraft(draft));

    expect(results).toMatchObject([{ ok: false, code: "queue_full" }, { ok: true }]);
    expect(closed.file.jobs.map((job) => job.id)).toEqual(["job-4"]);
    expect(closed.file.completed?.map((job) => job.jobId)).toEqual(["job-2"]);
  });

  it("leaves the source file untouched", () => {
    const source = runBatch(emptyQueue(Q, NOW), [push("job-1")], tokens()).file;
    const before = structuredClone(source);
    runBatch(source, [claim("worker-a", 1, NOW)], tokens("lease-a"));

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
    id: JobId.make(id),
    idempotencyKey: idempotencyKey === undefined ? undefined : IdempotencyKey.make(idempotencyKey),
    payload: { id },
    now: NOW,
  };
}

function claim(workerId: string, maxJobs: number, now: Instant): QueueMutation {
  return { kind: "claim", workerId: WorkerId.make(workerId), maxJobs, leaseSeconds: 60, now };
}

function complete(jobId: string, leaseToken: string, now: Instant): QueueMutation {
  return {
    kind: "complete",
    jobId: JobId.make(jobId),
    leaseToken: LeaseToken.make(leaseToken),
    retainSeconds: 60,
    now,
  };
}

// Hands out the given lease tokens in order, and fails the test if the model
// asks for more than it was given.
function tokens(...values: string[]): () => LeaseToken {
  const queue = values.map((value) => LeaseToken.make(value));

  return () => {
    const next = queue.shift();

    if (!next) throw new Error("model requested more lease tokens than the test provided");

    return next;
  };
}

function runBatch(...args: Parameters<typeof applyBatch>): BatchResult {
  return Effect.runSync(applyBatch(...args));
}
