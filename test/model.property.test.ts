import { Effect } from "effect";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  addSeconds,
  IdempotencyKey,
  Instant,
  isAtOrAfter,
  JobId,
  LeaseToken,
  QueueName,
  WorkerId,
} from "../src/domain";
import {
  applyBatch,
  emptyQueue,
  publicQueueFile,
  type MutationResult,
  type QueueFile,
  type QueueJob,
  type QueueMutation,
} from "../src/model";

const BASE = Instant.make("2026-08-31T12:00:00.000Z");

const LEASE_SECONDS = 30;

const RETAIN_SECONDS = 40;

const QUEUE = QueueName.make("property");

const GHOST_JOB = JobId.make("ghost-job");

const FORGED_TOKEN = LeaseToken.make("forged-token");

type Intent =
  | { kind: "push"; key: number | null; advance: number }
  | { kind: "claim"; worker: number; maxJobs: number; advance: number }
  | { kind: "heartbeat" | "complete"; job: number; token: number; advance: number };

const intentArbitrary: fc.Arbitrary<Intent> = fc.oneof(
  fc.record({
    kind: fc.constant("push" as const),
    key: fc.option(fc.nat(3)),
    advance: fc.nat(20),
  }),
  fc.record({
    kind: fc.constant("claim" as const),
    worker: fc.nat(2),
    maxJobs: fc.integer({ min: 1, max: 3 }),
    advance: fc.nat(20),
  }),
  fc.record({
    kind: fc.constantFrom("heartbeat" as const, "complete" as const),
    job: fc.nat(),
    token: fc.nat(),
    advance: fc.nat(20),
  }),
);

const workloadArbitrary = fc.array(intentArbitrary, { minLength: 1, maxLength: 40 });

interface Step {
  before: QueueFile;
  mutation: QueueMutation;
  result: MutationResult;
  after: QueueFile;
}

interface Run {
  mutations: QueueMutation[];
  steps: Step[];
}

describe("queue model properties", () => {
  it("keeps queue invariants after every mutation", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        for (const step of runSequentially(intents).steps) checkInvariants(step);
      }),
    );
  });

  it("accepts heartbeat and complete exactly when the token holds a live lease", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        for (const step of runSequentially(intents).steps) checkFencing(step);
      }),
    );
  });

  it("claims the earliest claimable jobs, up to maxJobs", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        for (const step of runSequentially(intents).steps) checkFifoClaim(step);
      }),
    );
  });

  it("loses no job: only a successful complete removes one", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        for (const step of runSequentially(intents).steps) checkConservation(step);
      }),
    );
  });

  it("enqueues a key again only once its job has completed and the retention has passed", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        checkIdempotency(runSequentially(intents).steps);
      }),
    );
  });

  it("returns lease tokens only from claims and heartbeats", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        const leaked = runSequentially(intents).steps.flatMap(({ after, result }) =>
          result.kind === "claim" || result.kind === "heartbeat"
            ? [JSON.stringify(publicQueueFile(after))]
            : [JSON.stringify(result), JSON.stringify(publicQueueFile(after))],
        );

        for (const text of leaked) expect(text).not.toMatch(/token-\d/);
      }),
    );
  });

  it("gives the same results as one group commit and as one mutation at a time", () => {
    fc.assert(
      fc.property(workloadArbitrary, (intents) => {
        const sequential = runSequentially(intents);

        const batch = Effect.runSync(
          applyBatch(emptyQueue(QUEUE, BASE), sequential.mutations, tokenSource()),
        );

        const last = sequential.steps.at(-1)?.after;

        expect(batch.results).toEqual(sequential.steps.map((step) => step.result));
        expect(batch.file.jobs).toEqual(last?.jobs);
        expect(batch.file.completed).toEqual(last?.completed);
        expect(batch.file.revision).toBe(batch.changed ? 1 : 0);
      }),
    );
  });
});

function runSequentially(intents: readonly Intent[]): Run {
  const newLeaseToken = tokenSource();
  const jobIds: JobId[] = [GHOST_JOB];
  const tokens: LeaseToken[] = [FORGED_TOKEN];
  const mutations: QueueMutation[] = [];
  const steps: Step[] = [];
  let file = emptyQueue(QUEUE, BASE);
  let now = BASE;

  for (const [index, intent] of intents.entries()) {
    now = addSeconds(now, intent.advance);

    const mutation = toMutation(intent, index, now, jobIds, tokens);
    const applied = Effect.runSync(applyBatch(file, [mutation], newLeaseToken));
    const result = applied.results[0];

    if (!result) throw new Error("applyBatch returned no result for a single mutation");

    if (result.kind === "push" && result.ok && !result.duplicate) jobIds.push(result.job.id);

    if (result.kind === "claim") tokens.push(...result.jobs.map((job) => job.lease.token));

    mutations.push(mutation);
    steps.push({ before: file, mutation, result, after: applied.file });
    file = applied.file;
  }

  return { mutations, steps };
}

function toMutation(
  intent: Intent,
  index: number,
  now: Instant,
  jobIds: readonly JobId[],
  tokens: readonly LeaseToken[],
): QueueMutation {
  switch (intent.kind) {
    case "push":
      return {
        kind: "push",
        id: JobId.make(`job-${index}`),
        idempotencyKey: intent.key === null ? undefined : IdempotencyKey.make(`key-${intent.key}`),
        payload: { index },
        now,
      };

    case "claim":
      return {
        kind: "claim",
        workerId: WorkerId.make(`worker-${intent.worker}`),
        maxJobs: intent.maxJobs,
        leaseSeconds: LEASE_SECONDS,
        now,
      };

    case "heartbeat":
    case "complete": {
      const jobId = jobIds[intent.job % jobIds.length] ?? GHOST_JOB;
      const leaseToken = tokens[intent.token % tokens.length] ?? FORGED_TOKEN;

      return intent.kind === "heartbeat"
        ? { kind: "heartbeat", jobId, leaseToken, leaseSeconds: LEASE_SECONDS, now }
        : { kind: "complete", jobId, leaseToken, retainSeconds: RETAIN_SECONDS, now };
    }
  }
}

function checkInvariants({ before, after }: Step): void {
  const ids = after.jobs.map((job) => job.id);

  const liveTokens = after.jobs.flatMap((job) => (job.state === "leased" ? [job.lease.token] : []));

  const keys = after.jobs.flatMap((job) =>
    job.idempotencyKey === undefined ? [] : [job.idempotencyKey],
  );

  expect(new Set(ids).size).toBe(ids.length);
  expect(new Set(liveTokens).size).toBe(liveTokens.length);
  expect(new Set(keys).size).toBe(keys.length);

  for (const job of after.jobs) {
    if (job.state === "pending") expect(job.attempts).toBe(0);
    else expect(job.attempts).toBeGreaterThanOrEqual(1);
  }

  expect([before.revision, before.revision + 1]).toContain(after.revision);

  if (JSON.stringify(before.jobs) !== JSON.stringify(after.jobs)) {
    expect(after.revision).toBe(before.revision + 1);
  }
}

function checkFencing({ before, mutation, result }: Step): void {
  if (mutation.kind !== "heartbeat" && mutation.kind !== "complete") return;

  const job = before.jobs.find((candidate) => candidate.id === mutation.jobId);

  const live =
    job?.state === "leased" &&
    job.lease.token === mutation.leaseToken &&
    !isAtOrAfter(mutation.now, job.lease.expiresAt);

  if (!job && mutation.kind === "complete") {
    const completed = before.completed?.find(
      (candidate) =>
        candidate.jobId === mutation.jobId && !isAtOrAfter(mutation.now, candidate.expiresAt),
    );

    expect(result.ok).toBe(completed?.leaseToken === mutation.leaseToken);
    expect(result).toMatchObject(
      result.ok
        ? { alreadyCompleted: true }
        : { code: completed ? "lease_mismatch" : "job_not_found" },
    );

    return;
  }

  expect(result.ok).toBe(live);
}

function checkFifoClaim({ before, mutation, result }: Step): void {
  if (mutation.kind !== "claim" || result.kind !== "claim") return;

  const claimable = before.jobs.flatMap((job) => (isClaimable(job, mutation.now) ? [job.id] : []));

  expect(result.jobs.map((job) => job.id)).toEqual(claimable.slice(0, mutation.maxJobs));
}

function checkConservation({ before, mutation, result, after }: Step): void {
  const expected = before.jobs.map((job) => job.id);

  if (result.kind === "push" && result.ok && !result.duplicate) expected.push(result.job.id);

  if (result.kind === "complete" && result.ok && !result.alreadyCompleted) {
    expected.splice(expected.indexOf(result.jobId), 1);
  }

  expect(after.jobs.map((job) => job.id)).toEqual(expected);
  expect(mutation.kind === "complete" || after.jobs.length >= before.jobs.length).toBe(true);
}

function checkIdempotency(steps: readonly Step[]): void {
  const history = new Map<string, { jobId: JobId; completedAt: Instant | undefined }>();

  for (const { mutation, result } of steps) {
    if (mutation.kind === "complete" && result.kind === "complete" && result.ok) {
      for (const entry of history.values()) {
        if (entry.jobId === result.jobId && !entry.completedAt) entry.completedAt = mutation.now;
      }
    }

    if (mutation.kind !== "push" || result.kind !== "push" || !mutation.idempotencyKey) continue;

    const previous = history.get(mutation.idempotencyKey);

    const retained =
      previous !== undefined &&
      (previous.completedAt === undefined ||
        !isAtOrAfter(mutation.now, addSeconds(previous.completedAt, RETAIN_SECONDS)));

    expect(result).toMatchObject({ ok: true, duplicate: retained });

    if (retained) {
      const jobId = "job" in result ? result.job.id : "jobId" in result ? result.jobId : undefined;

      expect(jobId).toBe(previous.jobId);
    } else if (result.ok && "job" in result) {
      history.set(mutation.idempotencyKey, { jobId: result.job.id, completedAt: undefined });
    }
  }
}

function isClaimable(job: QueueJob, now: Instant): boolean {
  return job.state === "pending" || isAtOrAfter(now, job.lease.expiresAt);
}

function tokenSource(): () => LeaseToken {
  let issued = 0;

  return () => {
    issued += 1;

    return LeaseToken.make(`token-${issued}`);
  };
}
