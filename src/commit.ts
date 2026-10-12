import { z } from "zod";
import { applyBatch, type QueueFile, type QueueMutation, type MutationResult } from "./model";

const StorageErrorSchema = z.object({ message: z.string() });

export interface CommitStore {
  load(force: boolean): Promise<{ file: QueueFile; etag: string | null }>;
  put(body: string, etag: string | null): Promise<string | null>;
}

export interface CommitDeps {
  store: CommitStore;
  pacer: CommitPacer;
  sleep(ms: number): Promise<void>;
  random(): number;
  newCommitId(): string;
  maxBytes: number;
  maxAttempts: number;
  backoffBaseMs: number;
  backoffCapMs: number;
}

export class StorageUnavailableError extends Error {
  constructor() {
    super("R2 remained unavailable after retrying the queue commit");
    this.name = "StorageUnavailableError";
  }
}

export function isRetryableStorageError(cause: unknown): boolean {
  const parsed = StorageErrorSchema.safeParse(cause);

  if (!parsed.success) return false;

  return /\((?:10058|10001|10043|10054)\)\s*$/u.test(parsed.data.message);
}

export function backoffMs(
  attempt: number,
  baseMs: number,
  capMs: number,
  random: () => number,
): number {
  return random() * Math.min(capMs, baseMs * 2 ** attempt);
}

export class CommitPacer {
  private lastStartAt: number | undefined;
  private pendingTurn: Promise<void> = Promise.resolve();

  constructor(
    private readonly intervalMs: number,
    private readonly now: () => number,
    private readonly sleep: (ms: number) => Promise<void>,
  ) {}

  waitReady(): Promise<void> {
    const ready = this.pendingTurn.then(() => this.waitUntilReady());

    this.pendingTurn = ready.catch(() => {});

    return ready;
  }

  waitTurn(): Promise<void> {
    const turn = this.pendingTurn.then(async () => {
      await this.waitUntilReady();
      this.lastStartAt = this.now();
    });

    this.pendingTurn = turn.catch(() => {});

    return turn;
  }

  private async waitUntilReady(): Promise<void> {
    if (this.lastStartAt === undefined) return;

    const waitMs = this.intervalMs - (this.now() - this.lastStartAt);

    if (waitMs > 0) await this.sleep(waitMs);
  }
}

export async function commitBatch(
  mutations: QueueMutation[],
  deps: CommitDeps,
): Promise<{ file: QueueFile; etag: string | null; results: MutationResult[] }> {
  const attempts = new Map<string, { file: QueueFile; results: MutationResult[] }>();
  let failedPutAttempt: number | undefined;
  let failedCommitId: string | undefined;

  for (let attempt = 0; attempt < deps.maxAttempts; attempt += 1) {
    let snapshot: { file: QueueFile; etag: string | null };

    try {
      snapshot = await deps.store.load(attempt > 0);
    } catch (error) {
      if (!isRetryableStorageError(error)) throw error;
      failedPutAttempt = undefined;

      if (attempt + 1 < deps.maxAttempts) {
        await deps.sleep(backoffMs(attempt, deps.backoffBaseMs, deps.backoffCapMs, deps.random));
      }

      continue;
    }

    const recovered = snapshot.file.commitId ? attempts.get(snapshot.file.commitId) : undefined;

    if (recovered) {
      return { file: snapshot.file, etag: snapshot.etag, results: recovered.results };
    }

    failedCommitId = undefined;

    if (failedPutAttempt !== undefined) {
      await deps.sleep(
        backoffMs(failedPutAttempt, deps.backoffBaseMs, deps.backoffCapMs, deps.random),
      );
      failedPutAttempt = undefined;
    }

    const commitId = deps.newCommitId();
    const rejectPushes = new Set<number>();
    let evictCompleted = 0;
    let applied = applyBatch(snapshot.file, mutations, rejectPushes, evictCompleted);

    if (!applied.changed) {
      return { file: applied.file, etag: snapshot.etag, results: applied.results };
    }

    applied.file.commitId = commitId;

    const encoder = new TextEncoder();
    let body = JSON.stringify(applied.file);
    let bodyBytes = encoder.encode(body).byteLength;

    while (bodyBytes > deps.maxBytes) {
      let overflow = bodyBytes - deps.maxBytes;
      let rejectedPush = false;

      for (let index = mutations.length - 1; index >= 0 && overflow > 0; index -= 1) {
        if (rejectPushes.has(index)) continue;

        const result = applied.results[index];

        if (!result?.ok || result.kind !== "push" || result.duplicate) continue;

        rejectPushes.add(index);
        rejectedPush = true;
        overflow -= encoder.encode(JSON.stringify(result.job)).byteLength + 1;
      }

      if (!rejectedPush) {
        let evictedBytes = 0;
        let evictedCount = 0;

        const oldestCompleted = applied.file.completed.toSorted(
          (first, second) => Date.parse(first.completedAt) - Date.parse(second.completedAt),
        );

        for (const completed of oldestCompleted) {
          if (evictedBytes >= overflow) break;

          evictedBytes += encoder.encode(JSON.stringify(completed)).byteLength + 1;
          evictedCount += 1;
        }

        if (evictedCount === 0) {
          throw new Error(`queue exceeds configured MAX_QUEUE_BYTES of ${deps.maxBytes}`);
        }

        evictCompleted += evictedCount;
      }

      applied = applyBatch(snapshot.file, mutations, rejectPushes, evictCompleted);

      if (!applied.changed) {
        return { file: applied.file, etag: snapshot.etag, results: applied.results };
      }

      applied.file.commitId = commitId;
      body = JSON.stringify(applied.file);
      bodyBytes = encoder.encode(body).byteLength;
    }

    attempts.set(commitId, { file: applied.file, results: applied.results });
    await deps.pacer.waitTurn();

    try {
      const etag = await deps.store.put(body, snapshot.etag);

      if (etag !== null) {
        return { file: applied.file, etag, results: applied.results };
      }
    } catch (error) {
      if (!isRetryableStorageError(error)) throw error;
      failedPutAttempt = attempt;
      failedCommitId = commitId;
    }
  }

  if (failedCommitId) {
    try {
      const snapshot = await deps.store.load(true);
      const recovered = snapshot.file.commitId ? attempts.get(snapshot.file.commitId) : undefined;

      if (recovered) {
        return { file: snapshot.file, etag: snapshot.etag, results: recovered.results };
      }
    } catch (error) {
      if (!isRetryableStorageError(error)) throw error;
    }
  }

  throw new StorageUnavailableError();
}
