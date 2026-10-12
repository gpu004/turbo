export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

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
): BatchResult {
  const file = structuredClone(source);
  const results: MutationResult[] = [];
  let changed = false;

  const batchNow = mutations.at(-1)?.now;
  if (batchNow) {
    const nowMs = Date.parse(batchNow);
    file.completed = file.completed.filter((job) => Date.parse(job.expiresAt) > nowMs);
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
          ...(mutation.idempotencyKey ? { idempotencyKey: mutation.idempotencyKey } : {}),
        };
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
          ...result.job.lease,
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

        file.completed.push({
          jobId: result.job.id,
          leaseToken: mutation.leaseToken,
          ...(result.job.idempotencyKey ? { idempotencyKey: result.job.idempotencyKey } : {}),
          completedAt: mutation.now,
          expiresAt: new Date(
            Date.parse(mutation.now) + mutation.retainSeconds * 1_000,
          ).toISOString(),
        });
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
    completed: file.completed.map((job) => ({
      jobId: job.jobId,
      ...(job.idempotencyKey ? { idempotencyKey: job.idempotencyKey } : {}),
      completedAt: job.completedAt,
      expiresAt: job.expiresAt,
    })),
  };
}

function validateLease(
  file: QueueFile,
  jobId: string,
  leaseToken: string,
  now: string,
):
  | { ok: true; job: QueueJob & { lease: Lease } }
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
  return { ok: true, job: job as QueueJob & { lease: Lease } };
}

export function assertQueueFile(value: unknown, expectedQueue: string): asserts value is QueueFile {
  if (!isRecord(value) || value.version !== 1 || value.queue !== expectedQueue) {
    throw new Error(`R2 object for queue ${expectedQueue} has an incompatible format`);
  }
  if (
    !Array.isArray(value.jobs) ||
    typeof value.revision !== "number" ||
    ("completed" in value && !Array.isArray(value.completed))
  ) {
    throw new Error(`R2 object for queue ${expectedQueue} is malformed`);
  }
}

export function normalizeQueueFile(value: unknown, expectedQueue: string): QueueFile {
  assertQueueFile(value, expectedQueue);
  return { ...value, completed: value.completed ?? [] };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
