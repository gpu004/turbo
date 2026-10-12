import { DurableObject } from "cloudflare:workers";
import { CommitPacer, StorageUnavailableError, commitBatch, type CommitDeps } from "./commit";
import {
  emptyQueue,
  JsonValueSchema,
  parseQueueFile,
  publicQueueFile,
  type BrokerFailure,
  type MutationResult,
  type QueueFile,
  type QueueMutation,
} from "./model";

interface Snapshot {
  file: QueueFile;
  etag: string | null;
}

interface PendingMutation {
  mutation: QueueMutation;
  resolve: (result: MutationResult | BrokerFailure) => void;
  reject: (cause: unknown) => void;
}

export class QueueBroker extends DurableObject<Env> {
  private snapshot: Snapshot | undefined;
  private queueName: string | undefined;
  private pending: PendingMutation[] = [];
  private flushPromise: Promise<void> | undefined;
  private readonly pacer: CommitPacer;
  private readonly commitDeps: CommitDeps;
  private readonly maxPendingMutations: number;
  private readonly retryAfterSeconds: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    const commitIntervalMs = this.integerSetting("QUEUE_COMMIT_INTERVAL_MS", 1_000, 0, 60_000);
    this.maxPendingMutations = this.integerSetting("MAX_PENDING_MUTATIONS", 10_000, 1, 1_000_000);
    this.retryAfterSeconds = Math.max(1, Math.ceil(commitIntervalMs / 1_000));
    this.pacer = new CommitPacer(commitIntervalMs, () => performance.now(), delay);
    this.commitDeps = {
      store: {
        load: async (force) => {
          const queue = this.queueName;

          if (!queue) throw new Error("queue broker is not initialized");

          return this.load(queue, force);
        },
        put: async (body, etag) => {
          const queue = this.queueName;

          if (!queue) throw new Error("queue broker is not initialized");

          const stored = await this.env.QUEUE_BUCKET.put(this.objectKey(queue), body, {
            onlyIf: etag ? { etagMatches: etag } : { etagDoesNotMatch: "*" },
            httpMetadata: { contentType: "application/json" },
          });

          return stored?.etag ?? null;
        },
      },
      pacer: this.pacer,
      sleep: delay,
      random: () => Math.random(),
      newCommitId: () => crypto.randomUUID(),
      maxBytes: this.integerSetting("MAX_QUEUE_BYTES", 8 * 1024 * 1024, 1_024, 32 * 1024 * 1024),
      maxAttempts: 5,
      backoffBaseMs: Math.max(commitIntervalMs, 50),
      backoffCapMs: 8_000,
    };
  }

  async push(queue: string, payloadJson: string, idempotencyKey?: string): Promise<string> {
    this.bindQueue(queue);

    const mutation: Extract<QueueMutation, { kind: "push" }> = {
      kind: "push",
      id: crypto.randomUUID(),
      payload: JsonValueSchema.parse(JSON.parse(payloadJson)),
      now: new Date().toISOString(),
    };

    if (idempotencyKey) mutation.idempotencyKey = idempotencyKey;
    const result = await this.enqueue(mutation);

    if (result.kind !== "push" && result.kind !== "unavailable") {
      throw new Error("broker returned the wrong mutation result");
    }

    return JSON.stringify(result);
  }

  async claim(queue: string, workerId: string, maxJobs: number): Promise<string> {
    this.bindQueue(queue);

    const result = await this.enqueue({
      kind: "claim",
      workerId,
      maxJobs,
      leaseTokens: Array.from({ length: maxJobs }, () => crypto.randomUUID()),
      leaseSeconds: this.integerSetting("QUEUE_LEASE_SECONDS", 60, 1, 86_400),
      now: new Date().toISOString(),
    });

    if (result.kind !== "claim" && result.kind !== "unavailable") {
      throw new Error("broker returned the wrong mutation result");
    }

    return JSON.stringify(result);
  }

  async heartbeat(queue: string, jobId: string, leaseToken: string): Promise<string> {
    this.bindQueue(queue);

    const result = await this.enqueue({
      kind: "heartbeat",
      jobId,
      leaseToken,
      leaseSeconds: this.integerSetting("QUEUE_LEASE_SECONDS", 60, 1, 86_400),
      now: new Date().toISOString(),
    });

    if (result.kind !== "heartbeat" && result.kind !== "unavailable") {
      throw new Error("broker returned the wrong mutation result");
    }

    return JSON.stringify(result);
  }

  async complete(queue: string, jobId: string, leaseToken: string): Promise<string> {
    this.bindQueue(queue);

    const result = await this.enqueue({
      kind: "complete",
      jobId,
      leaseToken,
      now: new Date().toISOString(),
      retainSeconds: this.integerSetting("COMPLETED_RETENTION_SECONDS", 3_600, 1, 604_800),
    });

    if (result.kind !== "complete" && result.kind !== "unavailable") {
      throw new Error("broker returned the wrong mutation result");
    }

    return JSON.stringify(result);
  }

  async inspect(queue: string): Promise<string> {
    this.bindQueue(queue);

    if (this.flushPromise) await this.flushPromise;
    const snapshot = await this.load(queue, true);

    return JSON.stringify(publicQueueFile(snapshot.file));
  }

  private bindQueue(queue: string): void {
    if (this.queueName && this.queueName !== queue) {
      throw new Error("Durable Object received operations for two queue names");
    }

    this.queueName = queue;
  }

  private enqueue(mutation: QueueMutation): Promise<MutationResult | BrokerFailure> {
    if (this.pending.length >= this.maxPendingMutations) {
      return Promise.resolve({
        ok: false,
        kind: "unavailable",
        code: "overloaded",
        message: "queue broker has reached the pending mutation limit",
        retryAfterSeconds: this.retryAfterSeconds,
      });
    }

    const result = new Promise<MutationResult | BrokerFailure>((resolve, reject) => {
      this.pending.push({ mutation, resolve, reject });
    });

    if (!this.flushPromise) {
      this.flushPromise = this.flushLoop().finally(() => {
        this.flushPromise = undefined;
      });
      this.ctx.waitUntil(this.flushPromise);
    }

    return result;
  }

  private async flushLoop(): Promise<void> {
    await delay(this.integerSetting("QUEUE_BATCH_WINDOW_MS", 5, 0, 1_000));

    while (this.pending.length > 0) {
      const batch = this.pending.splice(0);

      try {
        const results = await this.commit(batch.map((item) => item.mutation));
        batch.forEach((item, index) => {
          const result = results[index];

          if (result) item.resolve(result);
          else item.reject(new Error("broker returned an incomplete commit result"));
        });
      } catch (error) {
        if (error instanceof StorageUnavailableError) {
          const failure: BrokerFailure = {
            ok: false,
            kind: "unavailable",
            code: "storage_unavailable",
            message: error.message,
            retryAfterSeconds: this.retryAfterSeconds,
          };

          for (const item of batch) item.resolve(failure);
        } else {
          for (const item of batch) item.reject(error);
        }
      }
    }
  }

  private async commit(mutations: QueueMutation[]): Promise<MutationResult[]> {
    try {
      const committed = await commitBatch(mutations, this.commitDeps);
      this.snapshot = { file: committed.file, etag: committed.etag };

      return committed.results;
    } catch (error) {
      this.snapshot = undefined;
      throw error;
    }
  }

  private async load(queue: string, force = false): Promise<Snapshot> {
    if (this.snapshot && !force) return this.snapshot;

    const object = await this.env.QUEUE_BUCKET.get(this.objectKey(queue));

    if (!object) {
      const snapshot = { file: emptyQueue(queue, new Date().toISOString()), etag: null };
      this.snapshot = snapshot;

      return snapshot;
    }

    const snapshot = { file: parseQueueFile(await object.text(), queue), etag: object.etag };
    this.snapshot = snapshot;

    return snapshot;
  }

  private objectKey(queue: string): string {
    return `queues/${encodeURIComponent(queue)}/queue.json`;
  }

  private integerSetting(
    name:
      | "QUEUE_BATCH_WINDOW_MS"
      | "QUEUE_LEASE_SECONDS"
      | "MAX_QUEUE_BYTES"
      | "COMPLETED_RETENTION_SECONDS"
      | "QUEUE_COMMIT_INTERVAL_MS"
      | "MAX_PENDING_MUTATIONS",
    fallback: number,
    minimum: number,
    maximum: number,
  ): number {
    const parsed = Number.parseInt(this.env[name], 10);

    return Number.isFinite(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
