import { Data, Effect, Schema } from "effect";
import {
  addSeconds,
  CommitId,
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

export interface PendingJob extends JobFields {
  readonly state: "pending";
}

export interface LeasedJob extends JobFields {
  readonly state: "leased";
  readonly lease: Lease;
}

export type QueueJob = PendingJob | LeasedJob;

export interface PublicLease {
  readonly workerId: WorkerId;
  readonly heartbeatAt: Instant;
  readonly expiresAt: Instant;
  readonly token?: never;
}

export type PublicJob = PendingJob | (Omit<LeasedJob, "lease"> & { readonly lease: PublicLease });

export interface CompletedJob {
  readonly jobId: JobId;
  readonly leaseToken: LeaseToken;
  readonly idempotencyKey?: IdempotencyKey;
  readonly completedAt: Instant;
  readonly expiresAt: Instant;
}

export type PublicCompletedJob = Omit<CompletedJob, "leaseToken"> & { readonly leaseToken?: never };

export interface QueueFile {
  readonly version: 1;
  readonly queue: QueueName;
  readonly broker: {
    readonly kind: "durable-object";
    readonly name: QueueName;
  };
  readonly revision: number;
  readonly updatedAt: Instant;
  readonly commitId?: CommitId;
  readonly jobs: readonly QueueJob[];
  readonly completed?: readonly CompletedJob[];
}

export interface PublicQueueFile extends Omit<QueueFile, "jobs" | "completed"> {
  readonly jobs: readonly PublicJob[];
  readonly completed?: readonly PublicCompletedJob[];
}

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

const CompletedJobSchema = storedStruct({
  jobId: JobId,
  leaseToken: LeaseToken,
  idempotencyKey: Schema.optionalKey(IdempotencyKey),
  completedAt: Instant,
  expiresAt: Instant,
});

const QueueFileSchema: Schema.Decoder<QueueFile> = storedStruct({
  version: Schema.Literal(1),
  queue: QueueName,
  broker: storedStruct({
    kind: Schema.Literal("durable-object"),
    name: QueueName,
  }),
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  updatedAt: Instant,
  commitId: Schema.optionalKey(CommitId),
  jobs: Schema.Array(QueueJobSchema),
  completed: Schema.optionalKey(Schema.Array(CompletedJobSchema)),
}).check(
  Schema.makeFilter((file) => invariantViolation(file.jobs, supersede(file.jobs, file.completed))),
);

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

export function invariantViolation(
  jobs: readonly QueueJob[],
  completed: readonly CompletedJob[] = [],
): string | undefined {
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

  for (const job of completed) {
    if (ids.has(job.jobId)) return `job ${job.jobId} is both queued and completed`;
    ids.add(job.jobId);

    if (job.idempotencyKey !== undefined) {
      if (keys.has(job.idempotencyKey)) {
        return `idempotency key ${job.idempotencyKey} is used by two jobs`;
      }

      keys.add(job.idempotencyKey);
    }
  }

  return undefined;
}

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
    retainSeconds: number;
    now: Instant;
  };
}

export type LeaseFailureCode = "job_not_found" | "lease_mismatch" | "lease_expired";

export interface LeaseFailure {
  readonly ok: false;
  readonly code: LeaseFailureCode;
  readonly message: string;
}

export interface QueueFull {
  readonly ok: false;
  readonly code: "queue_full";
  readonly message: string;
}

interface MutationOutputs {
  push:
    | { readonly ok: true; readonly job: PublicJob; readonly duplicate: boolean }
    | {
        readonly ok: true;
        readonly duplicate: true;
        readonly jobId: JobId;
        readonly completedAt: Instant;
      }
    | QueueFull;
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

export interface QueueDraft {
  readonly source: QueueFile;
  readonly jobs: QueueJob[];
  readonly completed: CompletedJob[];
  readonly newLeaseToken: () => LeaseToken;
  readonly shed: Shedding;
  changedAt: Instant | undefined;
}

export interface Shedding {
  readonly refusedPushes: ReadonlySet<JobId>;
  readonly evictedCompleted: number;
}

const noShedding: Shedding = { refusedPushes: new Set(), evictedCompleted: 0 };

type Handlers = {
  [K in MutationKind]: (draft: QueueDraft, mutation: MutationOf<K>) => ResultOf<K>;
};

const handlers: Handlers = {
  push(draft, mutation) {
    const existing =
      mutation.idempotencyKey === undefined
        ? undefined
        : draft.jobs.find((job) => job.idempotencyKey === mutation.idempotencyKey);

    if (existing) return { ok: true, kind: "push", job: publicJob(existing), duplicate: true };

    const completedIndex =
      mutation.idempotencyKey === undefined
        ? -1
        : draft.completed.findIndex((job) => job.idempotencyKey === mutation.idempotencyKey);

    const completed = draft.completed[completedIndex];

    if (completed && !isAtOrAfter(mutation.now, completed.expiresAt)) {
      return {
        ok: true,
        kind: "push",
        duplicate: true,
        jobId: completed.jobId,
        completedAt: completed.completedAt,
      };
    }

    if (draft.shed.refusedPushes.has(mutation.id)) {
      return {
        ok: false,
        kind: "push",
        code: "queue_full",
        message: "queue exceeds configured MAX_QUEUE_BYTES",
      };
    }

    if (completed) draft.completed.splice(completedIndex, 1);

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
      return { ...completedBefore(draft, mutation), kind: "complete" };
    }

    const found = findLiveLease(draft, mutation.jobId, mutation.leaseToken, mutation.now);

    if (!found.ok) return { ...found, kind: "complete" };

    const fields: CompletedJob = {
      jobId: found.job.id,
      leaseToken: mutation.leaseToken,
      completedAt: mutation.now,
      expiresAt: addSeconds(mutation.now, mutation.retainSeconds),
    };

    draft.completed.push(
      found.job.idempotencyKey === undefined
        ? fields
        : { ...fields, idempotencyKey: found.job.idempotencyKey },
    );
    draft.jobs.splice(found.index, 1);
    draft.changedAt = mutation.now;

    return { ok: true, kind: "complete", jobId: mutation.jobId, alreadyCompleted: false };
  },
};

export function openDraft(
  source: QueueFile,
  newLeaseToken: () => LeaseToken,
  shed: Shedding = noShedding,
): QueueDraft {
  return {
    source,
    jobs: [...source.jobs],
    completed: supersede(source.jobs, source.completed),
    newLeaseToken,
    shed,
    changedAt: undefined,
  };
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

  const completed = draft.completed
    .filter((job) => !isAtOrAfter(changedAt, job.expiresAt))
    .slice(draft.shed.evictedCompleted);

  const violation = invariantViolation(draft.jobs, completed);

  if (violation) {
    return Effect.fail(
      new InvariantViolation({
        queue: draft.source.queue,
        message: `commit for queue ${draft.source.queue} would break an invariant: ${violation}`,
      }),
    );
  }

  const { completed: _previous, ...rest } = draft.source;

  const file: QueueFile = {
    ...rest,
    revision: draft.source.revision + 1,
    updatedAt: changedAt,
    jobs: draft.jobs,
  };

  return Effect.succeed({
    file: completed.length === 0 ? file : { ...file, completed },
    changed: true,
  });
}

export function planShedding(
  source: QueueFile,
  file: QueueFile,
  shed: Shedding,
  overflow: number,
): Shedding | undefined {
  let freed = 0;
  let evicted = 0;

  for (const job of file.completed ?? []) {
    if (freed >= overflow) break;

    freed += serializedBytes(job) + 1;
    evicted += 1;
  }

  if (evicted > 0) return { ...shed, evictedCompleted: shed.evictedCompleted + evicted };

  const existing = new Set(source.jobs.map((job) => job.id));
  const added = file.jobs.filter((job) => !existing.has(job.id));

  if (added.length === 0) return undefined;

  const sizes = added.map((job) => serializedBytes(job) + 1);
  let room = sizes.reduce((total, size) => total + size, 0) - overflow;
  const refused = new Set(shed.refusedPushes);

  for (const [index, job] of added.entries()) {
    const size = sizes[index] ?? 0;

    if (size <= room) room -= size;
    else refused.add(job.id);
  }

  return { ...shed, refusedPushes: refused };
}

export function serializedBytes(value: QueueFile | QueueJob | CompletedJob | JsonValue): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
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

function supersede(
  jobs: readonly QueueJob[],
  completed: readonly CompletedJob[] = [],
): CompletedJob[] {
  const keys = new Set(jobs.map((job) => job.idempotencyKey));

  return completed.filter(
    (job) => job.idempotencyKey === undefined || !keys.has(job.idempotencyKey),
  );
}

export function publicJob(job: QueueJob): PublicJob {
  if (job.state === "pending") return job;

  const { token: _token, ...lease } = job.lease;

  return { ...job, lease };
}

export function publicQueueFile(file: QueueFile): PublicQueueFile {
  const { completed, ...rest } = file;
  const shown: PublicQueueFile = { ...rest, jobs: file.jobs.map(publicJob) };

  if (completed === undefined) return shown;

  return { ...shown, completed: completed.map(({ leaseToken: _leaseToken, ...job }) => job) };
}

function completedBefore(
  draft: QueueDraft,
  mutation: MutationOf<"complete">,
): { ok: true; jobId: JobId; alreadyCompleted: true } | LeaseFailure {
  const completed = draft.completed.find((job) => job.jobId === mutation.jobId);

  if (!completed || isAtOrAfter(mutation.now, completed.expiresAt)) {
    return { ok: false, code: "job_not_found", message: "job does not exist" };
  }

  if (completed.leaseToken !== mutation.leaseToken) {
    return { ok: false, code: "lease_mismatch", message: "lease token does not own this job" };
  }

  return { ok: true, jobId: mutation.jobId, alreadyCompleted: true };
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
