import { Effect, Exit, Layer, Option } from "effect";
import { describe, expect, it } from "vitest";
import {
  Background,
  Broker,
  CommitAttemptsExhausted,
  QueueStore,
  type StoredQueue,
} from "../src/broker";
import { Settings, type SettingsEnv } from "../src/config";
import {
  IdempotencyKey,
  Ids,
  Instant,
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
  readonly file: () => QueueFile | undefined;
}

// `beforePut` runs before each write is checked, so a test can play a second
// writer that changes the object underneath the broker.
function memoryStore(
  beforePut: (write: (file: QueueFile) => void) => void = () => {},
): MemoryStore {
  let object: StoredQueue | undefined;
  let version = 0;
  let puts = 0;

  const write = (file: QueueFile): void => {
    version += 1;
    object = { text: JSON.stringify(file), etag: `etag-${version}` };
  };

  const layer = Layer.succeed(QueueStore, {
    get: () => Effect.sync(() => Option.fromNullishOr(object)),
    put: (_queue, body, etag) =>
      Effect.sync(() => {
        puts += 1;
        beforePut(write);

        const matches = Option.match(etag, {
          onNone: () => object === undefined,
          onSome: (expected) => object?.etag === expected,
        });

        if (!matches) return Option.none();

        version += 1;
        object = { text: body, etag: `etag-${version}` };

        return Option.some(object.etag);
      }),
  });

  return {
    layer,
    puts: () => puts,
    file: () => (object ? Effect.runSync(parseQueueFile(object.text, Q)) : undefined),
  };
}

function sequentialIds(): Layer.Layer<Ids> {
  let issued = 0;

  return Layer.succeed(Ids, {
    newJobId: () => {
      issued += 1;

      return JobId.make(`job-${issued}`);
    },
    newLeaseToken: () => {
      issued += 1;

      return LeaseToken.make(`lease-${issued}`);
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
    Settings.fromEnv({ QUEUE_BATCH_WINDOW_MS: "20", ...env }),
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
});
