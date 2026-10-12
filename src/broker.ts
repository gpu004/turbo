import { DurableObject } from "cloudflare:workers";
import {
  applyBatch,
  emptyQueue,
  parseQueueFile,
  JsonValueSchema,
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
  resolve: (result: MutationResult) => void;
  reject: (cause: unknown) => void;
}

export class QueueBroker extends DurableObject<Env> {
  private snapshot: Snapshot | undefined;
  private queueName: string | undefined;
  private pending: PendingMutation[] = [];
  private flushPromise: Promise<void> | undefined;

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

    if (result.kind !== "push") throw new Error("broker returned the wrong mutation result");

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

    if (result.kind !== "claim") throw new Error("broker returned the wrong mutation result");

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

    if (result.kind !== "heartbeat") throw new Error("broker returned the wrong mutation result");

    return JSON.stringify(result);
  }

  async complete(queue: string, jobId: string, leaseToken: string): Promise<string> {
    this.bindQueue(queue);

    const result = await this.enqueue({
      kind: "complete",
      jobId,
      leaseToken,
      now: new Date().toISOString(),
    });

    if (result.kind !== "complete") throw new Error("broker returned the wrong mutation result");

    return JSON.stringify(result);
  }

  async inspect(queue: string): Promise<string> {
    this.bindQueue(queue);

    if (this.flushPromise) await this.flushPromise;
    const snapshot = await this.load(queue, true);

    return JSON.stringify(snapshot.file);
  }

  private bindQueue(queue: string): void {
    if (this.queueName && this.queueName !== queue) {
      throw new Error("Durable Object received operations for two queue names");
    }

    this.queueName = queue;
  }

  private enqueue(mutation: QueueMutation): Promise<MutationResult> {
    const result = new Promise<MutationResult>((resolve, reject) => {
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
        for (const item of batch) item.reject(error);
      }
    }
  }

  private async commit(mutations: QueueMutation[]): Promise<MutationResult[]> {
    const queue = this.queueName;

    if (!queue) throw new Error("queue broker is not initialized");

    for (let attempt = 0; attempt < 5; attempt += 1) {
      const snapshot = await this.load(queue, attempt > 0);
      const applied = applyBatch(snapshot.file, mutations);

      if (!applied.changed) return applied.results;

      const body = JSON.stringify(applied.file);

      const maxBytes = this.integerSetting(
        "MAX_QUEUE_BYTES",
        8 * 1024 * 1024,
        1_024,
        32 * 1024 * 1024,
      );

      if (new TextEncoder().encode(body).byteLength > maxBytes) {
        throw new Error(`queue exceeds configured MAX_QUEUE_BYTES of ${maxBytes}`);
      }

      const stored = await this.env.QUEUE_BUCKET.put(this.objectKey(queue), body, {
        onlyIf: snapshot.etag ? { etagMatches: snapshot.etag } : { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      });

      if (stored) {
        this.snapshot = { file: applied.file, etag: stored.etag };

        return applied.results;
      }

      this.snapshot = undefined;
    }

    throw new Error("R2 compare-and-set failed after 5 attempts");
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
    name: "QUEUE_BATCH_WINDOW_MS" | "QUEUE_LEASE_SECONDS" | "MAX_QUEUE_BYTES",
    fallback: number,
    minimum: number,
    maximum: number,
  ): number {
    const parsed = Math.trunc(Number(this.env[name]));

    return Number.isFinite(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
