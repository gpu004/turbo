import { Data, Effect, Schema } from "effect";
import {
  addSeconds,
  IdempotencyKey,
  Instant,
  isAtOrAfter,
  JobId,
  JsonText,
  JsonValue,
  LeaseToken,
  QueueName,
  WorkerId,
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

// Stored objects keep fields this version does not know about, so a broker
// that rewrites a queue preserves fields a newer broker added.
function storedStruct<const Fields extends Schema.Struct.Fields>(fields: Fields) {
  return Schema.StructWithRest(Schema.Struct(fields), [Schema.Record(Schema.String, JsonValue)]);
}

const LeaseSchema = storedStruct({
  token: LeaseToken,
  workerId: WorkerId,
  heartbeatAt: Instant,
  expiresAt: Instant,
});

const jobFields = {
  id: JobId,
  payload: JsonValue,
  idempotencyKey: Schema.optionalKey(IdempotencyKey),
  enqueuedAt: Instant,
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
};

const QueueJobSchema = Schema.Union([
  storedStruct({ ...jobFields, state: Schema.Literal("pending") }),
  storedStruct({ ...jobFields, state: Schema.Literal("leased"), lease: LeaseSchema }),
]);

const QueueFileSchema: Schema.Decoder<QueueFile> = storedStruct({
  version: Schema.Literal(1),
  queue: QueueName,
  broker: storedStruct({
    kind: Schema.Literal("durable-object"),
    name: QueueName,
  }),
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  updatedAt: Instant,
  jobs: Schema.Array(QueueJobSchema),
}).check(Schema.makeFilter((file) => invariantViolation(file.jobs)));

// Only the identity fields, so a file for another queue or format version is
// reported as incompatible rather than malformed.
const QueueIdentitySchema = Schema.Struct({ version: Schema.Literal(1), queue: Schema.String });

export class MalformedQueueFile extends Data.TaggedError("MalformedQueueFile")<{
  readonly queue: QueueName;
  readonly message: string;
}> {}

export class IncompatibleQueueFile extends Data.TaggedError("IncompatibleQueueFile")<{
  readonly queue: QueueName;
  readonly message: string;
}> {}

export type QueueFileError = MalformedQueueFile | IncompatibleQueueFile;

export class InvariantViolation extends Data.TaggedError("InvariantViolation")<{
  readonly queue: QueueName;
  readonly message: string;
}> {}

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

export function closeDraft(draft: QueueDraft): Effect.Effect<ClosedDraft, InvariantViolation> {
  const changedAt = draft.changedAt;

  if (changedAt === undefined) return Effect.succeed({ file: draft.source, changed: false });

  const violation = invariantViolation(draft.jobs);

  if (violation) {
    return Effect.fail(
      new InvariantViolation({
        queue: draft.source.queue,
        message: `commit for queue ${draft.source.queue} would break an invariant: ${violation}`,
      }),
    );
  }

  return Effect.succeed({
    file: {
      ...draft.source,
      revision: draft.source.revision + 1,
      updatedAt: changedAt,
      jobs: draft.jobs,
    },
    changed: true,
  });
}

export interface BatchResult extends ClosedDraft {
  results: MutationResult[];
}

export function applyBatch(
  source: QueueFile,
  mutations: readonly QueueMutation[],
  newLeaseToken: () => LeaseToken,
): Effect.Effect<BatchResult, InvariantViolation> {
  const draft = openDraft(source, newLeaseToken);
  const results = mutations.map((mutation) => applyMutation(draft, mutation));

  return closeDraft(draft).pipe(Effect.map((closed) => ({ ...closed, results })));
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

export function parseQueueFile(
  text: string,
  expectedQueue: QueueName,
): Effect.Effect<QueueFile, QueueFileError> {
  const malformed = new MalformedQueueFile({
    queue: expectedQueue,
    message: `R2 object for queue ${expectedQueue} is malformed`,
  });

  const incompatible = new IncompatibleQueueFile({
    queue: expectedQueue,
    message: `R2 object for queue ${expectedQueue} has an incompatible format`,
  });

  return Effect.gen(function* () {
    const json = yield* Schema.decodeUnknownEffect(JsonText)(text).pipe(
      Effect.mapError(() => malformed),
    );

    const identity = yield* Schema.decodeUnknownEffect(QueueIdentitySchema)(json).pipe(
      Effect.mapError(() => incompatible),
    );

    if (identity.queue !== expectedQueue) return yield* incompatible;

    return yield* Schema.decodeUnknownEffect(QueueFileSchema)(json).pipe(
      Effect.mapError(() => malformed),
    );
  });
}
