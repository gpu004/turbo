import { describe, expect, it } from "vitest";
import {
  backoffMs,
  CommitPacer,
  commitBatch,
  isRetryableStorageError,
  StorageUnavailableError,
  type CommitDeps,
  type CommitStore,
} from "../src/commit";
import {
  applyBatch,
  emptyQueue,
  parseQueueFile,
  type QueueFile,
  type QueueMutation,
} from "../src/model";

const NOW = "2026-08-31T12:00:00.000Z";

describe("queue commit retries", () => {
  it("retries a rate-limited write with bounded jittered backoff", async () => {
    const store = new MemoryStore();
    const acceptPut = store.putBehavior;

    store.putBehavior = async (body, etag) => {
      if (store.putCalls === 1) throw new Error("put: rate limit exceeded (10058)");

      return acceptPut(body, etag);
    };

    const clock = new FakeClock();
    const committed = await commitBatch([push("job-1")], dependencies(store, clock));

    expect(store.putCalls).toBe(2);
    expect(clock.sleeps).toHaveLength(1);
    expect(clock.sleeps[0]).toBeGreaterThanOrEqual(0);
    expect(clock.sleeps[0]).toBeLessThanOrEqual(100);
    expect(committed.results[0]).toMatchObject({ ok: true, kind: "push", job: { id: "job-1" } });
  });

  it("recovers an ambiguous landed write from its commit ID", async () => {
    const store = new MemoryStore();
    const acceptPut = store.putBehavior;

    store.putBehavior = async (body, etag) => {
      const accepted = await acceptPut(body, etag);

      if (store.putCalls === 1) throw new Error("put: rate limit exceeded (10058)");

      return accepted;
    };

    const committed = await commitBatch([push("job-1")], dependencies(store, new FakeClock()));

    expect(store.putCalls).toBe(1);
    expect(store.storedWrites).toBe(1);
    expect(store.file.jobs.map((job) => job.id)).toEqual(["job-1"]);
    expect(committed.results[0]).toMatchObject({ ok: true, kind: "push", job: { id: "job-1" } });
  });

  it("rethrows non-retryable storage errors after one put", async () => {
    const store = new MemoryStore();
    store.putBehavior = async () => {
      throw new Error("put: unauthorized (10002)");
    };

    await expect(
      commitBatch([push("job-1")], dependencies(store, new FakeClock())),
    ).rejects.toThrow("(10002)");

    expect(store.putCalls).toBe(1);
  });

  it("throws StorageUnavailableError after exhausting retryable failures", async () => {
    const store = new MemoryStore();
    store.putBehavior = async () => {
      throw new Error("put: rate limit exceeded (10058)");
    };

    await expect(
      commitBatch([push("job-1")], dependencies(store, new FakeClock())),
    ).rejects.toBeInstanceOf(StorageUnavailableError);

    expect(store.putCalls).toBe(5);
  });

  it("counts retryable load failures as failed attempts", async () => {
    const store = new MemoryStore();
    store.loadBehavior = async () => {
      throw new Error("load: service unavailable (10043)");
    };

    await expect(
      commitBatch([push("job-1")], dependencies(store, new FakeClock())),
    ).rejects.toBeInstanceOf(StorageUnavailableError);

    expect(store.loadCalls).toBe(5);
    expect(store.putCalls).toBe(0);
  });

  it("reloads and reapplies after a compare-and-set loss without backoff", async () => {
    const store = new MemoryStore();
    const acceptPut = store.putBehavior;
    let firstPut = true;

    store.putBehavior = async (body, etag) => {
      if (firstPut) {
        firstPut = false;
        store.file = applyBatch(store.file, [push("concurrent")]).file;
        store.etag = "etag-concurrent";

        return null;
      }

      return acceptPut(body, etag);
    };

    const clock = new FakeClock();
    const committed = await commitBatch([push("job-1")], dependencies(store, clock));

    expect(store.putCalls).toBe(2);
    expect(store.file.jobs.map((job) => job.id)).toEqual(["concurrent", "job-1"]);
    expect(clock.sleeps).toEqual([]);
    expect(committed.results[0]).toMatchObject({ ok: true, kind: "push", job: { id: "job-1" } });
  });

  it("paces three consecutive put starts by the configured interval", async () => {
    const clock = new FakeClock();
    const pacer = new CommitPacer(1_000, clock.now, clock.sleep);
    const starts: number[] = [];

    await Promise.all(
      [0, 1, 2].map(async () => {
        await pacer.waitTurn();
        starts.push(clock.time);
      }),
    );

    expect(starts).toEqual([0, 1_000, 2_000]);
  });

  it("waitReady does not advance the next turn before the first put", async () => {
    const clock = new FakeClock();
    const pacer = new CommitPacer(1_000, clock.now, clock.sleep);

    await pacer.waitReady();
    await pacer.waitTurn();

    expect(clock.time).toBe(0);
    expect(clock.sleeps).toEqual([]);
  });

  it("waitReady waits for the remaining interval without claiming the turn", async () => {
    const clock = new FakeClock();
    const pacer = new CommitPacer(1_000, clock.now, clock.sleep);

    await pacer.waitTurn();
    clock.time = 300;
    await pacer.waitReady();

    expect(clock.time).toBe(1_000);
    expect(clock.sleeps).toEqual([700]);

    await pacer.waitTurn();

    expect(clock.sleeps).toEqual([700]);
  });

  it("classifies retryable R2 errors and bounds full-jitter backoff", () => {
    const cases: Array<{ code: string; retryable: boolean }> = [
      { code: "10058", retryable: true },
      { code: "10001", retryable: true },
      { code: "10043", retryable: true },
      { code: "10054", retryable: true },
      { code: "10031", retryable: false },
      { code: "10002", retryable: false },
      { code: "100100", retryable: false },
      { code: "99999", retryable: false },
    ];

    for (const testCase of cases) {
      expect(isRetryableStorageError(new Error(`R2 request failed (${testCase.code})`))).toBe(
        testCase.retryable,
      );
    }

    expect(isRetryableStorageError({ message: "R2 request failed (10058)" })).toBe(true);
    expect(isRetryableStorageError("R2 request failed (10058)")).toBe(false);
    expect(backoffMs(4, 100, 250, () => 0)).toBe(0);
    expect(backoffMs(4, 100, 250, () => 1)).toBe(250);
  });
});

class MemoryStore implements CommitStore {
  file = emptyQueue("q", NOW);
  etag: string | null = null;
  loadCalls = 0;
  putCalls = 0;
  storedWrites = 0;
  loadBehavior: (force: boolean) => Promise<{ file: QueueFile; etag: string | null }> =
    async () => ({
      file: structuredClone(this.file),
      etag: this.etag,
    });
  putBehavior: (body: string, etag: string | null) => Promise<string | null> = async (
    body,
    etag,
  ) => {
    if (etag !== this.etag) return null;

    this.file = parseQueueFile(body, this.file.queue);
    this.storedWrites += 1;
    this.etag = `etag-${this.storedWrites}`;

    return this.etag;
  };

  async load(force: boolean): Promise<{ file: QueueFile; etag: string | null }> {
    this.loadCalls += 1;

    return this.loadBehavior(force);
  }

  async put(body: string, etag: string | null): Promise<string | null> {
    this.putCalls += 1;

    return this.putBehavior(body, etag);
  }
}

class FakeClock {
  time = 0;
  sleeps: number[] = [];

  now = (): number => this.time;

  sleep = async (milliseconds: number): Promise<void> => {
    this.sleeps.push(milliseconds);
    this.time += milliseconds;
  };
}

function dependencies(store: CommitStore, clock: FakeClock): CommitDeps {
  let commitNumber = 0;

  return {
    store,
    pacer: new CommitPacer(0, clock.now, clock.sleep),
    sleep: clock.sleep,
    random: () => 0.5,
    newCommitId: () => {
      commitNumber += 1;

      return `commit-${commitNumber}`;
    },
    maxBytes: 100_000,
    maxAttempts: 5,
    backoffBaseMs: 100,
    backoffCapMs: 8_000,
  };
}

function push(id: string): QueueMutation {
  return { kind: "push", id, payload: { id }, now: NOW };
}
