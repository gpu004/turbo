import { z } from "zod";
import {
  addSeconds,
  IdempotencyKey,
  Instant,
  isAtOrAfter,
  JobId,
  JsonValueSchema,
  LeaseToken,
  parseJson,
  QueueName,
  WorkerId,
  type JsonValue,
} from "./domain";

export interface Lease {
  readonly token: LeaseToken;
  readonly workerId: WorkerId;
  readonly heartbeatAt: Instant;
  readonly expiresAt: Instant;
}

interface JobFields {
  readonly id: JobId;
  readonly payload: JsonValue;
  readonly idempotencyKey?: IdempotencyKey;
  readonly enqueuedAt: Instant;
  readonly attempts: number;
}

// A job either has a lease or it does not. There is no pending job with a
// lease and no leased job without one.
export interface PendingJob extends JobFields {
  readonly state: "pending";
}

export interface LeasedJob extends JobFields {
  readonly state: "leased";
  readonly lease: Lease;
}

export type QueueJob = PendingJob | LeasedJob;

export interface QueueFile {
  readonly version: 1;
  readonly queue: QueueName;
  readonly broker: {
    readonly kind: "durable-object";
    readonly name: QueueName;
  };
  readonly revision: number;
  readonly updatedAt: Instant;
  readonly jobs: readonly QueueJob[];
}

const LeaseSchema = z
  .object({
    token: LeaseToken,
    workerId: WorkerId,
    heartbeatAt: Instant,
    expiresAt: Instant,
  })
  .loose();

const jobFields = {
  id: JobId,
  payload: JsonValueSchema,
  idempotencyKey: IdempotencyKey.exactOptional(),
  enqueuedAt: Instant,
  attempts: z.number().int().min(0),
};

const QueueJobSchema: z.ZodType<QueueJob> = z.discriminatedUnion("state", [
  z.object({ ...jobFields, state: z.literal("pending") }).loose(),
  z.object({ ...jobFields, state: z.literal("leased"), lease: LeaseSchema }).loose(),
]);

const QueueFileSchema: z.ZodType<QueueFile> = z
  .object({
    version: z.literal(1),
    queue: QueueName,
    broker: z
      .object({
        kind: z.literal("durable-object"),
        name: QueueName,
      })
      .loose(),
    revision: z.number().int().min(0),
    updatedAt: Instant,
    jobs: z.array(QueueJobSchema),
  })
  .loose()
  .superRefine((file, context) => {
    const violation = invariantViolation(file.jobs);

    if (violation) context.addIssue({ code: "custom", message: violation });
  });

// Invariants the types cannot express. They are checked when a queue is
// loaded from R2 and again in `closeDraft`, before a file can be written, so a
// model bug fails the commit instead of persisting a corrupt queue.
export function invariantViolation(jobs: readonly QueueJob[]): string | undefined {
  const ids = new Set<JobId>();
  const keys = new Set<IdempotencyKey>();
  const tokens = new Set<LeaseToken>();

  for (const job of jobs) {
    if (ids.has(job.id)) return `job ${job.id} appears twice`;
    ids.add(job.id);

    if (job.idempotencyKey !== undefined) {
      if (keys.has(job.idempotencyKey)) {
        return `idempotency key ${job.idempotencyKey} is used by two jobs`;
      }

      keys.add(job.idempotencyKey);
    }

    if (job.state === "pending") {
      if (job.attempts !== 0) return `pending job ${job.id} has ${job.attempts} attempts`;
      continue;
    }

    if (job.attempts < 1) return `leased job ${job.id} has no attempts`;

    if (isAtOrAfter(job.lease.heartbeatAt, job.lease.expiresAt)) {
      return `lease on job ${job.id} expires before its last heartbeat`;
    }

    if (tokens.has(job.lease.token)) return `lease token on job ${job.id} is used by two jobs`;
    tokens.add(job.lease.token);
  }

  return undefined;
}

// Each mutation kind maps to its input and its result. Adding a kind means
// adding one entry to each map and one handler; the compiler finds the rest.

interface MutationInputs {
  push: {
    id: JobId;
    idempotencyKey: IdempotencyKey | undefined;
    payload: JsonValue;
    now: Instant;
  };
  claim: {
    workerId: WorkerId;
    maxJobs: number;
    leaseSeconds: number;
    now: Instant;
  };
  heartbeat: {
    jobId: JobId;
    leaseToken: LeaseToken;
    leaseSeconds: number;
    now: Instant;
  };
  complete: {
    jobId: JobId;
    leaseToken: LeaseToken;
    now: Instant;
  };
}

export type LeaseFailureCode = "job_not_found" | "lease_mismatch" | "lease_expired";

export interface LeaseFailure {
  readonly ok: false;
  readonly code: LeaseFailureCode;
  readonly message: string;
}

interface MutationOutputs {
  push: { readonly ok: true; readonly job: QueueJob; readonly duplicate: boolean };
  claim: { readonly ok: true; readonly jobs: readonly LeasedJob[] };
  heartbeat: { readonly ok: true; readonly job: LeasedJob } | LeaseFailure;
  complete:
    | { readonly ok: true; readonly jobId: JobId; readonly alreadyCompleted: boolean }
    | LeaseFailure;
}

export type MutationKind = keyof MutationInputs;

export type MutationOf<K extends MutationKind = MutationKind> = {
  [P in K]: { readonly kind: P } & MutationInputs[P];
}[K];

export type ResultOf<K extends MutationKind = MutationKind> = {
  [P in K]: { readonly kind: P } & MutationOutputs[P];
}[K];

export type QueueMutation = MutationOf;

export type MutationResult = ResultOf;

// A draft is the working copy of one queue file during one group commit.
// Jobs are immutable values; handlers replace array entries rather than
// mutating jobs, so results handed back to callers never change underneath them.
export interface QueueDraft {
  readonly source: QueueFile;
  readonly jobs: QueueJob[];
  readonly newLeaseToken: () => LeaseToken;
  changedAt: Instant | undefined;
}

type Handlers = {
  [K in MutationKind]: (draft: QueueDraft, mutation: MutationOf<K>) => ResultOf<K>;
};

const handlers: Handlers = {
  push(draft, mutation) {
    const existing =
      mutation.idempotencyKey === undefined
        ? undefined
        : draft.jobs.find((job) => job.idempotencyKey === mutation.idempotencyKey);

    if (existing) return { ok: true, kind: "push", job: existing, duplicate: true };

    const fields: PendingJob = {
      id: mutation.id,
      payload: mutation.payload,
      enqueuedAt: mutation.now,
      attempts: 0,
      state: "pending",
    };

    const job: PendingJob =
      mutation.idempotencyKey === undefined
        ? fields
        : { ...fields, idempotencyKey: mutation.idempotencyKey };

    draft.jobs.push(job);
    draft.changedAt = mutation.now;

    return { ok: true, kind: "push", job, duplicate: false };
  },

  claim(draft, mutation) {
    const claimed: LeasedJob[] = [];

    for (const [index, job] of draft.jobs.entries()) {
      if (claimed.length >= mutation.maxJobs) break;

      if (!isClaimable(job, mutation.now)) continue;

      const leased: LeasedJob = {
        ...job,
        state: "leased",
        attempts: job.attempts + 1,
        lease: {
          token: draft.newLeaseToken(),
          workerId: mutation.workerId,
          heartbeatAt: mutation.now,
          expiresAt: addSeconds(mutation.now, mutation.leaseSeconds),
        },
      };

      draft.jobs[index] = leased;
      claimed.push(leased);
      draft.changedAt = mutation.now;
    }

    return { ok: true, kind: "claim", jobs: claimed };
  },

  heartbeat(draft, mutation) {
    const found = findLiveLease(draft, mutation.jobId, mutation.leaseToken, mutation.now);

    if (!found.ok) return { ...found, kind: "heartbeat" };

    const job: LeasedJob = {
      ...found.job,
      lease: {
        ...found.job.lease,
        heartbeatAt: mutation.now,
        expiresAt: addSeconds(mutation.now, mutation.leaseSeconds),
      },
    };

    draft.jobs[found.index] = job;
    draft.changedAt = mutation.now;

    return { ok: true, kind: "heartbeat", job };
  },

  complete(draft, mutation) {
    if (!draft.jobs.some((job) => job.id === mutation.jobId)) {
      return { ok: true, kind: "complete", jobId: mutation.jobId, alreadyCompleted: true };
    }

    const found = findLiveLease(draft, mutation.jobId, mutation.leaseToken, mutation.now);

    if (!found.ok) return { ...found, kind: "complete" };

    draft.jobs.splice(found.index, 1);
    draft.changedAt = mutation.now;

    return { ok: true, kind: "complete", jobId: mutation.jobId, alreadyCompleted: false };
  },
};

export function openDraft(source: QueueFile, newLeaseToken: () => LeaseToken): QueueDraft {
  return { source, jobs: [...source.jobs], newLeaseToken, changedAt: undefined };
}

export function applyMutation<K extends MutationKind>(
  draft: QueueDraft,
  mutation: MutationOf<K>,
): ResultOf<K> {
  const handler: Handlers[K] = handlers[mutation.kind];

  return handler(draft, mutation);
}

export interface ClosedDraft {
  file: QueueFile;
  changed: boolean;
}

export function closeDraft(draft: QueueDraft): ClosedDraft {
  if (draft.changedAt === undefined) return { file: draft.source, changed: false };

  const violation = invariantViolation(draft.jobs);

  if (violation) {
    throw new Error(
      `commit for queue ${draft.source.queue} would break an invariant: ${violation}`,
    );
  }

  return {
    file: {
      ...draft.source,
      revision: draft.source.revision + 1,
      updatedAt: draft.changedAt,
      jobs: draft.jobs,
    },
    changed: true,
  };
}

export interface BatchResult extends ClosedDraft {
  results: MutationResult[];
}

export function applyBatch(
  source: QueueFile,
  mutations: readonly QueueMutation[],
  newLeaseToken: () => LeaseToken,
): BatchResult {
  const draft = openDraft(source, newLeaseToken);
  const results = mutations.map((mutation) => applyMutation(draft, mutation));

  return { ...closeDraft(draft), results };
}

export function emptyQueue(queue: QueueName, now: Instant): QueueFile {
  return {
    version: 1,
    queue,
    broker: { kind: "durable-object", name: queue },
    revision: 0,
    updatedAt: now,
    jobs: [],
  };
}

function isClaimable(job: QueueJob, now: Instant): boolean {
  return job.state === "pending" || isAtOrAfter(now, job.lease.expiresAt);
}

function findLiveLease(
  draft: QueueDraft,
  jobId: JobId,
  leaseToken: LeaseToken,
  now: Instant,
): { ok: true; index: number; job: LeasedJob } | LeaseFailure {
  const index = draft.jobs.findIndex((candidate) => candidate.id === jobId);
  const job = draft.jobs[index];

  if (!job || job.state !== "leased") {
    return { ok: false, code: "job_not_found", message: "job is not leased" };
  }

  if (job.lease.token !== leaseToken) {
    return { ok: false, code: "lease_mismatch", message: "lease token does not own this job" };
  }

  if (isAtOrAfter(now, job.lease.expiresAt)) {
    return { ok: false, code: "lease_expired", message: "job lease has expired" };
  }

  return { ok: true, index, job };
}

export function parseQueueFile(text: string, expectedQueue: QueueName): QueueFile {
  const json = parseJson(text);

  if (!json.ok) throw new Error(`R2 object for queue ${expectedQueue} is malformed`);

  const identity = z
    .object({ version: z.literal(1), queue: z.string() })
    .loose()
    .safeParse(json.value);

  if (!identity.success || identity.data.queue !== expectedQueue) {
    throw new Error(`R2 object for queue ${expectedQueue} has an incompatible format`);
  }

  const file = QueueFileSchema.safeParse(json.value);

  if (!file.success) {
    throw new Error(`R2 object for queue ${expectedQueue} is malformed`);
  }

  return file.data;
}
