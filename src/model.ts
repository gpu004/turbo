import { z } from "zod";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export const JsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.null(),
    z.boolean(),
    z.number(),
    z.string(),
    z.array(JsonValueSchema),
    z.record(z.string(), JsonValueSchema),
  ]),
);

export interface Lease {
  token: string;
  workerId: string;
  heartbeatAt: string;
  expiresAt: string;
}

export interface QueueJob {
  id: string;
  payload: JsonValue;
  idempotencyKey?: string;
  enqueuedAt: string;
  attempts: number;
  state: "pending" | "leased";
  lease?: Lease;
}

export interface CompletedJob {
  jobId: string;
  leaseToken: string;
  idempotencyKey?: string;
  completedAt: string;
  expiresAt: string;
}

export interface QueueFile {
  version: 1;
  queue: string;
  broker: {
    kind: "durable-object";
    name: string;
  };
  revision: number;
  updatedAt: string;
  commitId?: string;
  jobs: QueueJob[];
  completed: CompletedJob[];
}

export type PublicJob = Omit<QueueJob, "lease"> & {
  lease?: Omit<Lease, "token">;
};

export type PublicQueueFile = Omit<QueueFile, "jobs" | "completed"> & {
  jobs: PublicJob[];
  completed: Array<Omit<CompletedJob, "leaseToken">>;
};

const PublicLeaseSchema: z.ZodType<Omit<Lease, "token">> = z.object({
  workerId: z.string(),
  heartbeatAt: z.string(),
  expiresAt: z.string(),
});

const LeaseSchema: z.ZodType<Lease> = z
  .object({
    token: z.string(),
    workerId: z.string(),
    heartbeatAt: z.string(),
    expiresAt: z.string(),
  })
  .loose();

const PublicJobSchema: z.ZodType<PublicJob> = z
  .object({
    id: z.string(),
    payload: JsonValueSchema,
    idempotencyKey: z.string().exactOptional(),
    enqueuedAt: z.string(),
    attempts: z.number(),
    state: z.enum(["pending", "leased"]),
    lease: PublicLeaseSchema.exactOptional(),
  })
  .loose();

const QueueJobSchema: z.ZodType<QueueJob> = z
  .object({
    id: z.string(),
    payload: JsonValueSchema,
    idempotencyKey: z.string().exactOptional(),
    enqueuedAt: z.string(),
    attempts: z.number(),
    state: z.enum(["pending", "leased"]),
    lease: LeaseSchema.exactOptional(),
  })
  .loose();

const CompletedJobSchema: z.ZodType<CompletedJob> = z
  .object({
    jobId: z.string(),
    leaseToken: z.string(),
    idempotencyKey: z.string().exactOptional(),
    completedAt: z.string(),
    expiresAt: z.string(),
  })
  .loose();

const QueueFileSchema: z.ZodType<QueueFile> = z
  .object({
    version: z.literal(1),
    queue: z.string(),
    broker: z
      .object({
        kind: z.literal("durable-object"),
        name: z.string(),
      })
      .loose(),
    revision: z.number(),
    updatedAt: z.string(),
    commitId: z.string().exactOptional(),
    jobs: z.array(QueueJobSchema),
    completed: z.array(CompletedJobSchema).default([]),
  })
  .loose();

export type QueueMutation =
  | {
      kind: "push";
      id: string;
      idempotencyKey?: string;
      payload: JsonValue;
      now: string;
    }
  | {
      kind: "claim";
      workerId: string;
      maxJobs: number;
      leaseTokens: string[];
      now: string;
      leaseSeconds: number;
    }
  | {
      kind: "heartbeat";
      jobId: string;
      leaseToken: string;
      now: string;
      leaseSeconds: number;
    }
  | {
      kind: "complete";
      jobId: string;
      leaseToken: string;
      now: string;
      retainSeconds: number;
    };

export type MutationResult =
  | { ok: true; kind: "push"; job: PublicJob; duplicate: boolean }
  | {
      ok: true;
      kind: "push";
      duplicate: true;
      jobId: string;
      completedAt: string;
    }
  | {
      ok: false;
      kind: "push";
      code: "queue_full";
      message: string;
    }
  | { ok: true; kind: "claim"; jobs: QueueJob[] }
  | { ok: true; kind: "heartbeat"; job: QueueJob }
  | { ok: true; kind: "complete"; jobId: string; alreadyCompleted: boolean }
  | {
      ok: false;
      kind: "heartbeat";
      code: "job_not_found" | "lease_mismatch" | "lease_expired";
      message: string;
    }
  | {
      ok: false;
      kind: "complete";
      code: "job_not_found" | "lease_mismatch" | "lease_expired";
      message: string;
    };

export type BrokerFailure = {
  ok: false;
  kind: "unavailable";
  code: "overloaded" | "storage_unavailable";
  message: string;
  retryAfterSeconds: number;
};

const MutationFailureSchema = z.object({
  ok: z.literal(false),
  code: z.enum(["job_not_found", "lease_mismatch", "lease_expired"]),
  message: z.string(),
});

export const BrokerFailureSchema: z.ZodType<BrokerFailure> = z.object({
  ok: z.literal(false),
  kind: z.literal("unavailable"),
  code: z.enum(["overloaded", "storage_unavailable"]),
  message: z.string(),
  retryAfterSeconds: z.number(),
});

export const PushResultSchema: z.ZodType<
  Extract<MutationResult, { kind: "push" }> | BrokerFailure
> = z.union([
  z.object({
    ok: z.literal(true),
    kind: z.literal("push"),
    job: PublicJobSchema,
    duplicate: z.boolean(),
  }),
  z.object({
    ok: z.literal(true),
    kind: z.literal("push"),
    duplicate: z.literal(true),
    jobId: z.string(),
    completedAt: z.string(),
  }),
  z.object({
    ok: z.literal(false),
    kind: z.literal("push"),
    code: z.literal("queue_full"),
    message: z.string(),
  }),
  BrokerFailureSchema,
]);

export const ClaimResultSchema: z.ZodType<
  Extract<MutationResult, { kind: "claim" }> | BrokerFailure
> = z.union([
  z.object({
    ok: z.literal(true),
    kind: z.literal("claim"),
    jobs: z.array(QueueJobSchema),
  }),
  BrokerFailureSchema,
]);

export const HeartbeatResultSchema: z.ZodType<
  Extract<MutationResult, { kind: "heartbeat" }> | BrokerFailure
> = z.union([
  z.object({
    ok: z.literal(true),
    kind: z.literal("heartbeat"),
    job: QueueJobSchema,
  }),
  MutationFailureSchema.extend({ kind: z.literal("heartbeat") }),
  BrokerFailureSchema,
]);

export const CompleteResultSchema: z.ZodType<
  Extract<MutationResult, { kind: "complete" }> | BrokerFailure
> = z.union([
  z.object({
    ok: z.literal(true),
    kind: z.literal("complete"),
    jobId: z.string(),
    alreadyCompleted: z.boolean(),
  }),
  MutationFailureSchema.extend({ kind: z.literal("complete") }),
  BrokerFailureSchema,
]);

export interface BatchResult {
  file: QueueFile;
  results: MutationResult[];
  changed: boolean;
}

export function emptyQueue(queue: string, now: string): QueueFile {
  return {
    version: 1,
    queue,
    broker: { kind: "durable-object", name: queue },
    revision: 0,
    updatedAt: now,
    jobs: [],
    completed: [],
  };
}

export function applyBatch(
  source: QueueFile,
  mutations: QueueMutation[],
  rejectPushes: ReadonlySet<number> = new Set(),
  evictCompleted = 0,
): BatchResult {
  const file = structuredClone(source);
  const results: MutationResult[] = [];
  let changed = false;

  let earliestMutationMs: number | undefined;

  for (const mutation of mutations) {
    const mutationMs = Date.parse(mutation.now);

    if (earliestMutationMs === undefined || mutationMs < earliestMutationMs) {
      earliestMutationMs = mutationMs;
    }
  }

  if (earliestMutationMs !== undefined) {
    file.completed = file.completed.filter((job) => Date.parse(job.expiresAt) > earliestMutationMs);
  }

  if (evictCompleted > 0) {
    const oldestCompleted = file.completed
      .toSorted((first, second) => Date.parse(first.completedAt) - Date.parse(second.completedAt))
      .slice(0, evictCompleted);

    const evictedCompleted = new Set(oldestCompleted);

    file.completed = file.completed.filter((job) => !evictedCompleted.has(job));
    changed = oldestCompleted.length > 0;
  }

  for (const [index, mutation] of mutations.entries()) {
    switch (mutation.kind) {
      case "push": {
        if (rejectPushes.has(index)) {
          results.push({
            ok: false,
            kind: "push",
            code: "queue_full",
            message: "queue exceeds configured MAX_QUEUE_BYTES",
          });
          break;
        }

        const existing = mutation.idempotencyKey
          ? file.jobs.find((job) => job.idempotencyKey === mutation.idempotencyKey)
          : undefined;

        if (existing) {
          results.push({ ok: true, kind: "push", job: publicJob(existing), duplicate: true });
          break;
        }

        const completed = mutation.idempotencyKey
          ? file.completed.find(
              (job) =>
                job.idempotencyKey === mutation.idempotencyKey &&
                Date.parse(job.expiresAt) > Date.parse(mutation.now),
            )
          : undefined;

        if (completed) {
          results.push({
            ok: true,
            kind: "push",
            duplicate: true,
            jobId: completed.jobId,
            completedAt: completed.completedAt,
          });
          break;
        }

        const job: QueueJob = {
          id: mutation.id,
          payload: mutation.payload,
          enqueuedAt: mutation.now,
          attempts: 0,
          state: "pending",
        };

        if (mutation.idempotencyKey) job.idempotencyKey = mutation.idempotencyKey;

        file.jobs.push(job);
        results.push({ ok: true, kind: "push", job: publicJob(job), duplicate: false });
        changed = true;
        break;
      }

      case "claim": {
        const nowMs = Date.parse(mutation.now);
        const claimed: QueueJob[] = [];

        for (const job of file.jobs) {
          if (claimed.length >= mutation.maxJobs) break;

          const ready =
            job.state === "pending" ||
            (job.lease !== undefined && Date.parse(job.lease.expiresAt) <= nowMs);

          if (!ready) continue;

          const token = mutation.leaseTokens[claimed.length];

          if (!token) throw new Error("claim mutation does not contain enough lease tokens");
          job.state = "leased";
          job.attempts += 1;
          job.lease = {
            token,
            workerId: mutation.workerId,
            heartbeatAt: mutation.now,
            expiresAt: new Date(nowMs + mutation.leaseSeconds * 1_000).toISOString(),
          };
          claimed.push(structuredClone(job));
          changed = true;
        }

        results.push({ ok: true, kind: "claim", jobs: claimed });
        break;
      }

      case "heartbeat": {
        const result = validateLease(file, mutation.jobId, mutation.leaseToken, mutation.now);

        if (!result.ok) {
          results.push({ ...result, kind: "heartbeat" });
          break;
        }

        const nowMs = Date.parse(mutation.now);
        result.job.lease = {
          ...result.lease,
          heartbeatAt: mutation.now,
          expiresAt: new Date(nowMs + mutation.leaseSeconds * 1_000).toISOString(),
        };
        results.push({
          ok: true,
          kind: "heartbeat",
          job: structuredClone(result.job),
        });
        changed = true;
        break;
      }

      case "complete": {
        const jobIndex = file.jobs.findIndex((job) => job.id === mutation.jobId);

        if (jobIndex === -1) {
          const completed = file.completed.find((job) => job.jobId === mutation.jobId);

          if (completed?.leaseToken === mutation.leaseToken) {
            results.push({
              ok: true,
              kind: "complete",
              jobId: mutation.jobId,
              alreadyCompleted: true,
            });
            break;
          }

          if (completed) {
            results.push({
              ok: false,
              kind: "complete",
              code: "lease_mismatch",
              message: "lease token does not own this job",
            });
            break;
          }

          results.push({
            ok: false,
            kind: "complete",
            code: "job_not_found",
            message: "job does not exist",
          });
          break;
        }

        const result = validateLease(file, mutation.jobId, mutation.leaseToken, mutation.now);

        if (!result.ok) {
          results.push({ ...result, kind: "complete" });
          break;
        }

        const completed: CompletedJob = {
          jobId: result.job.id,
          leaseToken: mutation.leaseToken,
          completedAt: mutation.now,
          expiresAt: new Date(
            Date.parse(mutation.now) + mutation.retainSeconds * 1_000,
          ).toISOString(),
        };

        if (result.job.idempotencyKey) completed.idempotencyKey = result.job.idempotencyKey;
        file.completed.push(completed);
        file.jobs.splice(jobIndex, 1);
        results.push({
          ok: true,
          kind: "complete",
          jobId: mutation.jobId,
          alreadyCompleted: false,
        });
        changed = true;
        break;
      }
    }
  }

  if (changed) {
    file.revision += 1;
    file.updatedAt = mutations.at(-1)?.now ?? file.updatedAt;
  }

  return { file, results, changed };
}

export function publicJob(job: QueueJob): PublicJob {
  if (!job.lease) return { ...job };

  return {
    ...job,
    lease: {
      workerId: job.lease.workerId,
      heartbeatAt: job.lease.heartbeatAt,
      expiresAt: job.lease.expiresAt,
    },
  };
}

export function publicQueueFile(file: QueueFile): PublicQueueFile {
  return {
    ...file,
    jobs: file.jobs.map(publicJob),
    completed: file.completed.map((job) => {
      const publicCompleted: Omit<CompletedJob, "leaseToken"> = {
        jobId: job.jobId,
        completedAt: job.completedAt,
        expiresAt: job.expiresAt,
      };

      if (job.idempotencyKey) publicCompleted.idempotencyKey = job.idempotencyKey;

      return publicCompleted;
    }),
  };
}

function validateLease(
  file: QueueFile,
  jobId: string,
  leaseToken: string,
  now: string,
):
  | { ok: true; job: QueueJob; lease: Lease }
  | {
      ok: false;
      code: "job_not_found" | "lease_mismatch" | "lease_expired";
      message: string;
    } {
  const job = file.jobs.find((candidate) => candidate.id === jobId);

  if (!job || job.state !== "leased" || !job.lease) {
    return { ok: false, code: "job_not_found", message: "job is not leased" };
  }

  if (job.lease.token !== leaseToken) {
    return { ok: false, code: "lease_mismatch", message: "lease token does not own this job" };
  }

  if (Date.parse(job.lease.expiresAt) <= Date.parse(now)) {
    return { ok: false, code: "lease_expired", message: "job lease has expired" };
  }

  return { ok: true, job, lease: job.lease };
}

export function parseQueueFile(text: string, expectedQueue: string): QueueFile {
  let value: unknown;

  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`R2 object for queue ${expectedQueue} is malformed`);
  }

  const identity = z
    .object({ version: z.literal(1), queue: z.string() })
    .loose()
    .safeParse(value);

  if (!identity.success || identity.data.queue !== expectedQueue) {
    throw new Error(`R2 object for queue ${expectedQueue} has an incompatible format`);
  }

  const file = QueueFileSchema.safeParse(value);

  if (!file.success) {
    throw new Error(`R2 object for queue ${expectedQueue} is malformed`);
  }

  return file.data;
}
