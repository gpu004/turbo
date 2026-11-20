import { DurableObject } from "cloudflare:workers";
import { parseConfig, type Config } from "./config";
import {
  currentInstant,
  newJobId,
  newLeaseToken,
  type IdempotencyKey,
  type JobId,
  type LeaseToken,
  type JsonValue,
  type QueueName,
  type WorkerId,
} from "./domain";
import { decode, encode, type Encoded } from "./rpc";
import {
  applyMutation,
  closeDraft,
  emptyQueue,
  openDraft,
  parseQueueFile,
  type MutationKind,
  type MutationOf,
  type QueueDraft,
  type QueueFile,
  type ResultOf,
} from "./model";

interface Snapshot {
  file: QueueFile;
  etag: string | null;
}

// A queued mutation, erased to a uniform type so different kinds can share one
// batch. `apply` runs the mutation against a draft and returns the callback
// that delivers its typed result; the result type never leaves the closure.
interface PendingCommand {
  apply: (draft: QueueDraft) => () => void;
  fail: (cause: Error) => void;
}

const MAX_COMMIT_ATTEMPTS = 5;

export class QueueBroker extends DurableObject<Env> {
  private readonly config: Config;
  private snapshot: Snapshot | undefined;
  private queueName: QueueName | undefined;
  private pending: PendingCommand[] = [];
  private flushPromise: Promise<void> | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.config = parseConfig(env);
  }

  async push(
    queue: QueueName,
    payload: Encoded<JsonValue>,
    idempotencyKey: IdempotencyKey | undefined,
  ): Promise<Encoded<ResultOf<"push">>> {
    this.bindQueue(queue);

    return encode(
      await this.enqueue({
        kind: "push",
        id: newJobId(),
        idempotencyKey,
        payload: decode(payload),
        now: currentInstant(),
      }),
    );
  }

  async claim(
    queue: QueueName,
    workerId: WorkerId,
    maxJobs: number,
  ): Promise<Encoded<ResultOf<"claim">>> {
    this.bindQueue(queue);

    return encode(
      await this.enqueue({
        kind: "claim",
        workerId,
        maxJobs,
        leaseSeconds: this.config.leaseSeconds,
        now: currentInstant(),
      }),
    );
  }

  async heartbeat(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"heartbeat">>> {
    this.bindQueue(queue);

    return encode(
      await this.enqueue({
        kind: "heartbeat",
        jobId,
        leaseToken,
        leaseSeconds: this.config.leaseSeconds,
        now: currentInstant(),
      }),
    );
  }

  async complete(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"complete">>> {
    this.bindQueue(queue);

    return encode(
      await this.enqueue({ kind: "complete", jobId, leaseToken, now: currentInstant() }),
    );
  }

  async inspect(queue: QueueName): Promise<Encoded<QueueFile>> {
    this.bindQueue(queue);

    if (this.flushPromise) await this.flushPromise;
    const snapshot = await this.load(queue, true);

    return encode(snapshot.file);
  }

  private bindQueue(queue: QueueName): void {
    if (this.queueName && this.queueName !== queue) {
      throw new Error("Durable Object received operations for two queue names");
    }

    this.queueName = queue;
  }

  private enqueue<K extends MutationKind>(mutation: MutationOf<K>): Promise<ResultOf<K>> {
    const result = new Promise<ResultOf<K>>((resolve, reject) => {
      this.pending.push({
        apply: (draft) => {
          const outcome = applyMutation(draft, mutation);

          return () => resolve(outcome);
        },
        fail: reject,
      });
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
    await delay(this.config.batchWindowMs);

    while (this.pending.length > 0) {
      const batch = this.pending.splice(0);

      try {
        const deliveries = await this.commit(batch);

        for (const deliver of deliveries) deliver();
      } catch (error) {
        const cause = error instanceof Error ? error : new Error(String(error));

        for (const command of batch) command.fail(cause);
      }
    }
  }

  // Applies the batch to the latest snapshot and writes it with an R2
  // compare-and-set. Results are delivered only after R2 accepts the write.
  private async commit(batch: readonly PendingCommand[]): Promise<Array<() => void>> {
    const queue = this.queueName;

    if (!queue) throw new Error("queue broker is not initialized");

    for (let attempt = 0; attempt < MAX_COMMIT_ATTEMPTS; attempt += 1) {
      const snapshot = await this.load(queue, attempt > 0);
      const draft = openDraft(snapshot.file, newLeaseToken);
      const deliveries = batch.map((command) => command.apply(draft));
      const { file, changed } = closeDraft(draft);

      if (!changed) return deliveries;

      const body = JSON.stringify(file);

      if (new TextEncoder().encode(body).byteLength > this.config.maxQueueBytes) {
        throw new Error(`queue exceeds configured MAX_QUEUE_BYTES of ${this.config.maxQueueBytes}`);
      }

      const stored = await this.env.QUEUE_BUCKET.put(objectKey(queue), body, {
        onlyIf: snapshot.etag ? { etagMatches: snapshot.etag } : { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      });

      if (stored) {
        this.snapshot = { file, etag: stored.etag };

        return deliveries;
      }

      this.snapshot = undefined;
    }

    throw new Error(`R2 compare-and-set failed after ${MAX_COMMIT_ATTEMPTS} attempts`);
  }

  private async load(queue: QueueName, force: boolean): Promise<Snapshot> {
    if (this.snapshot && !force) return this.snapshot;

    const object = await this.env.QUEUE_BUCKET.get(objectKey(queue));

    const snapshot: Snapshot = object
      ? { file: parseQueueFile(await object.text(), queue), etag: object.etag }
      : { file: emptyQueue(queue, currentInstant()), etag: null };

    this.snapshot = snapshot;

    return snapshot;
  }
}

function objectKey(queue: QueueName): string {
  return `queues/${encodeURIComponent(queue)}/queue.json`;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
