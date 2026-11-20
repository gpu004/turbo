import { Clock, Effect, Exit, Layer, Option } from "effect";
import { describe, expect, it } from "vitest";
import {
  Background,
  Broker,
  CommitAttemptsExhausted,
  Overloaded,
  QueueStore,
  StorageFailure,
  type StoredQueue,
} from "../src/broker";
import { Settings, type SettingsEnv } from "../src/config";
import {
  addSeconds,
  CommitId,
  currentInstant,
  IdempotencyKey,
  Ids,
  Instant,
  isAtOrAfter,
  JobId,
  LeaseToken,
  QueueName,
  WorkerId,
} from "../src/domain";
import { emptyQueue, parseQueueFile, type QueueFile } from "../src/model";

// The commit protocol in src/broker.ts against an in-memory R2 object with the
// same compare-and-set rule. spec/QueueBroker.tla model-checks the protocol;
// these tests check that the Effect implementation follows it.

const Q = QueueName.make("broker");

const NOW = Instant.make("2026-08-31T12:00:00.000Z");

interface MemoryStore {
  readonly layer: Layer.Layer<QueueStore>;
  readonly puts: () => number;
  readonly putTimes: () => readonly number[];
  readonly file: () => QueueFile | undefined;
}

// How the store fails the nth put (counting from 1): before writing, or after
// writing, as when R2 applies a write but the response is lost.
type PutFailure = (put: number) => { readonly landed: boolean; readonly code: number } | undefined;

// `beforePut` runs before each write is checked, so a test can play a second
// writer that changes the object underneath the broker.
function memoryStore(
  beforePut: (write: (file: QueueFile) => void) => void = () => {},
  failPut: PutFailure = () => undefined,
): MemoryStore {
  let object: StoredQueue | undefined;
  let version = 0;
  let puts = 0;
  const putTimes: number[] = [];

  const write = (file: QueueFile): void => {
    version += 1;
    object = { text: JSON.stringify(file), etag: `etag-${version}` };
  };

  const layer = Layer.succeed(QueueStore, {
    get: () => Effect.sync(() => Option.fromNullishOr(object)),
    put: (_queue, body, etag) =>
      Effect.gen(function* () {
        puts += 1;
        putTimes.push(yield* Clock.currentTimeMillis);
        beforePut(write);

        const failure = failPut(puts);

        const error = new StorageFailure({
          message: `put failed: R2 error (${failure?.code})`,
          retryable: [10058, 10001, 10043, 10054].includes(failure?.code ?? 0),
        });

        if (failure && !failure.landed) return yield* error;

        const matches = Option.match(etag, {
          onNone: () => object === undefined,
          onSome: (expected) => object?.etag === expected,
        });

        if (!matches) return Option.none();

        version += 1;
        object = { text: body, etag: `etag-${version}` };

        if (failure) return yield* error;

        return Option.some(object.etag);
      }),
  });

  return {
    layer,
    puts: () => puts,
    putTimes: () => putTimes,
    file: () => (object ? Effect.runSync(parseQueueFile(object.text, Q)) : undefined),
  };
}

// Commit ids have their own counter, so job and lease numbers stay the same
// however many commit attempts a test makes.
function sequentialIds(): Layer.Layer<Ids> {
  let issued = 0;
  let commits = 0;

  return Layer.succeed(Ids, {
    newJobId: () => {
      issued += 1;

      return JobId.make(`job-${issued}`);
    },
    newLeaseToken: () => {
      issued += 1;

      return LeaseToken.make(`lease-${issued}`);
    },
    newCommitId: () => {
      commits += 1;

      return CommitId.make(`commit-${commits}`);
    },
  });
}

const forkedBackground = Layer.succeed(Background, {
  run: (work) => Effect.forkDetach(work).pipe(Effect.asVoid),
});

function runBroker<A, E>(
  store: MemoryStore,
  program: Effect.Effect<A, E, Broker>,
  env: SettingsEnv = {},
): Promise<Exit.Exit<A, E>> {
  const services = Layer.mergeAll(
    Settings.fromEnv({ QUEUE_BATCH_WINDOW_MS: "20", QUEUE_COMMIT_INTERVAL_MS: "0", ...env }),
    sequentialIds(),
    store.layer,
    forkedBackground,
  );

  return Effect.runPromiseExit(
    program.pipe(Effect.provide(Broker.layer.pipe(Layer.provide(services)))),
  );
}

describe("queue broker", () => {
  it("commits concurrent mutations in one conditional write", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all([broker.push(Q, { n: 1 }, undefined), broker.push(Q, { n: 2 }, undefined)], {
          concurrency: "unbounded",
        }),
      ),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    expect(store.puts()).toBe(1);
    expect(store.file()?.revision).toBe(1);
    expect(store.file()?.jobs).toHaveLength(2);
  });

  it("reloads and reapplies the batch when another writer wins the compare-and-set", async () => {
    let raced = false;

    const store = memoryStore((write) => {
      if (raced) return;
      raced = true;

      const other = emptyQueue(Q, NOW);

      write({
        ...other,
        revision: 1,
        jobs: [
          {
            id: JobId.make("other"),
            payload: null,
            enqueuedAt: NOW,
            attempts: 0,
            state: "pending",
          },
        ],
      });
    });

    const exit = await runBroker(
      store,
      Broker.use((broker) => broker.push(Q, { n: 1 }, undefined)),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    expect(store.puts()).toBe(2);
    expect(store.file()?.revision).toBe(2);
    expect(store.file()?.jobs.map((job) => job.id)).toEqual(["other", "job-1"]);
  });

  it("fails every command in the batch after five lost compare-and-sets", async () => {
    let version = 0;

    const store = memoryStore((write) => {
      version += 1;
      write({ ...emptyQueue(Q, NOW), revision: version });
    });

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all(
          [
            Effect.flip(broker.push(Q, { n: 1 }, undefined)),
            Effect.flip(broker.push(Q, { n: 2 }, undefined)),
          ],
          { concurrency: "unbounded" },
        ),
      ),
    );

    expect(store.puts()).toBe(5);
    expect(exit).toEqual(
      Exit.succeed([expect.any(CommitAttemptsExhausted), expect.any(CommitAttemptsExhausted)]),
    );
  });

  it("refuses only the pushes that do not fit and commits the rest of the batch", async () => {
    const store = memoryStore();
    const tiny = { MAX_QUEUE_BYTES: "1024" };

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all(
          [
            broker.push(Q, { n: 1 }, undefined),
            broker.push(Q, "x".repeat(900), undefined),
            broker.push(Q, { n: 3 }, undefined),
          ],
          { concurrency: "unbounded" },
        ),
      ),
      tiny,
    );

    expect(exit).toEqual(
      Exit.succeed([
        expect.objectContaining({ ok: true, duplicate: false }),
        expect.objectContaining({ ok: false, code: "queue_full" }),
        expect.objectContaining({ ok: true, duplicate: false }),
      ]),
    );
    expect(store.puts()).toBe(1);
    expect(store.file()?.jobs.map((job) => job.payload)).toEqual([{ n: 1 }, { n: 3 }]);
  });

  it("evicts the oldest completed jobs to make room for new work", async () => {
    const store = memoryStore();
    const tiny = { MAX_QUEUE_BYTES: "1024" };

    const fill = Broker.use(
      Effect.fn(function* (broker) {
        for (let n = 0; n < 4; n += 1) {
          yield* broker.push(Q, { n }, IdempotencyKey.make(`key-${n}`));

          const [job] = (yield* broker.claim(Q, WorkerId.make("w"), 1)).jobs;

          if (!job) throw new Error("expected a claimed job");
          yield* broker.complete(Q, job.id, job.lease.token);
        }

        const retained = (yield* broker.inspect(Q)).completed?.length;
        const pushed = yield* broker.push(Q, "x".repeat(400), undefined);
        const claimed = yield* broker.claim(Q, WorkerId.make("w"), 1);

        return { retained, pushed, claimed };
      }),
    );

    const exit = await runBroker(store, fill, tiny);

    expect(exit).toEqual(
      Exit.succeed({
        retained: 4,
        pushed: expect.objectContaining({ ok: true, duplicate: false }),
        claimed: expect.objectContaining({ jobs: [expect.anything()] }),
      }),
    );

    const completed = store.file()?.completed?.map((job) => job.idempotencyKey) ?? [];

    expect(completed.length).toBeGreaterThan(0);
    expect(completed.length).toBeLessThan(4);
    expect(completed).toEqual(["key-0", "key-1", "key-2", "key-3"].slice(4 - completed.length));
  });

  it("waits QUEUE_COMMIT_INTERVAL_MS between writes and batches what arrives meanwhile", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        broker
          .push(Q, { n: 1 }, undefined)
          .pipe(
            Effect.andThen(
              Effect.all(
                [broker.push(Q, { n: 2 }, undefined), broker.push(Q, { n: 3 }, undefined)],
                { concurrency: "unbounded" },
              ),
            ),
          ),
      ),
      { QUEUE_COMMIT_INTERVAL_MS: "300" },
    );

    const [first = 0, second = 0] = store.putTimes();

    expect(Exit.isSuccess(exit)).toBe(true);
    expect(store.puts()).toBe(2);
    expect(second - first).toBeGreaterThanOrEqual(295);
    expect(store.file()?.jobs).toHaveLength(3);
  });

  it("retries a write that R2 rejects with a retryable error", async () => {
    const store = memoryStore(undefined, (put) =>
      put === 1 ? { landed: false, code: 10058 } : undefined,
    );

    const exit = await runBroker(
      store,
      Broker.use((broker) => broker.push(Q, { n: 1 }, undefined)),
    );

    expect(exit).toEqual(Exit.succeed(expect.objectContaining({ ok: true, duplicate: false })));
    expect(store.puts()).toBe(2);
    expect(store.file()?.jobs).toHaveLength(1);
  });

  it("delivers a write that landed although R2 reported an error, without applying it twice", async () => {
    const store = memoryStore(undefined, (put) =>
      put === 1 ? { landed: true, code: 10001 } : undefined,
    );

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all([broker.push(Q, { n: 1 }, undefined), broker.push(Q, { n: 2 }, undefined)], {
          concurrency: "unbounded",
        }),
      ),
    );

    expect(exit).toEqual(
      Exit.succeed([
        expect.objectContaining({ ok: true, job: expect.objectContaining({ id: "job-1" }) }),
        expect.objectContaining({ ok: true, job: expect.objectContaining({ id: "job-2" }) }),
      ]),
    );
    expect(store.puts()).toBe(1);
    expect(store.file()?.revision).toBe(1);
    expect(store.file()?.jobs.map((job) => job.id)).toEqual(["job-1", "job-2"]);
  });

  it("does not retry an R2 error that will not go away", async () => {
    const store = memoryStore(undefined, () => ({ landed: false, code: 10007 }));

    const exit = await runBroker(
      store,
      Broker.use((broker) => Effect.flip(broker.push(Q, { n: 1 }, undefined))),
    );

    expect(exit).toEqual(Exit.succeed(expect.any(StorageFailure)));
    expect(store.puts()).toBe(1);
  });

  it("refuses mutations once MAX_PENDING_MUTATIONS are waiting", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all(
          [broker.push(Q, { n: 1 }, undefined), Effect.flip(broker.push(Q, { n: 2 }, undefined))],
          { concurrency: "unbounded" },
        ),
      ),
      { MAX_PENDING_MUTATIONS: "1" },
    );

    expect(exit).toEqual(
      Exit.succeed([expect.objectContaining({ ok: true }), expect.any(Overloaded)]),
    );
    expect(store.file()?.jobs).toHaveLength(1);
  });

  it("starts a lease when its commit is written, not when the claim arrived", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use(
        Effect.fn(function* (broker) {
          yield* broker.push(Q, { n: 1 }, undefined);

          const requested = yield* currentInstant;
          const [job] = (yield* broker.claim(Q, WorkerId.make("w"), 1)).jobs;

          return { requested, heartbeatAt: job?.lease.heartbeatAt };
        }),
      ),
      { QUEUE_COMMIT_INTERVAL_MS: "300" },
    );

    if (!Exit.isSuccess(exit) || !exit.value.heartbeatAt) throw new Error("claim failed");

    const { requested, heartbeatAt } = exit.value;

    expect(isAtOrAfter(heartbeatAt, addSeconds(requested, 0.25))).toBe(true);
  });

  it("refuses pushes once MAX_QUEUE_BYTES of payloads are waiting", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use((broker) =>
        Effect.all(
          [
            broker.push(Q, "x".repeat(600), undefined),
            Effect.flip(broker.push(Q, "y".repeat(600), undefined)),
          ],
          { concurrency: "unbounded" },
        ),
      ),
      { MAX_QUEUE_BYTES: "1024" },
    );

    expect(exit).toEqual(
      Exit.succeed([expect.objectContaining({ ok: true }), expect.any(Overloaded)]),
    );
  });

  it("starts a retried claim's lease after the retry waits for its write", async () => {
    let puts = 0;

    const store = memoryStore((write) => {
      puts += 1;

      if (puts !== 2) return;

      write({
        ...emptyQueue(Q, NOW),
        revision: 5,
        jobs: [
          {
            id: JobId.make("other"),
            payload: null,
            enqueuedAt: NOW,
            attempts: 0,
            state: "pending",
          },
        ],
      });
    });

    const exit = await runBroker(
      store,
      Broker.use(
        Effect.fn(function* (broker) {
          yield* broker.push(Q, { n: 1 }, undefined);

          const requested = yield* currentInstant;
          const [job] = (yield* broker.claim(Q, WorkerId.make("w"), 1)).jobs;

          return { requested, heartbeatAt: job?.lease.heartbeatAt };
        }),
      ),
      { QUEUE_COMMIT_INTERVAL_MS: "300" },
    );

    if (!Exit.isSuccess(exit) || !exit.value.heartbeatAt) throw new Error("claim failed");

    const { requested, heartbeatAt } = exit.value;

    expect(store.puts()).toBe(3);
    expect(isAtOrAfter(heartbeatAt, addSeconds(requested, 0.55))).toBe(true);
  });

  it("completes a job right after claiming it with the shortest lease and production pacing", async () => {
    const store = memoryStore();

    const exit = await runBroker(
      store,
      Broker.use(
        Effect.fn(function* (broker) {
          yield* broker.push(Q, { n: 1 }, undefined);

          const [job] = (yield* broker.claim(Q, WorkerId.make("w"), 1)).jobs;

          if (!job) throw new Error("expected a claimed job");

          return yield* broker.complete(Q, job.id, job.lease.token);
        }),
      ),
      { QUEUE_LEASE_SECONDS: "1", QUEUE_COMMIT_INTERVAL_MS: "1000" },
    );

    expect(exit).toEqual(
      Exit.succeed(expect.objectContaining({ ok: true, alreadyCompleted: false })),
    );
  }, 10_000);
});
