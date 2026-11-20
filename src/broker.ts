import { DurableObject } from "cloudflare:workers";
import {
  Cause,
  Context,
  Data,
  Deferred,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Predicate,
  Ref,
} from "effect";
import { Settings } from "./config";
import {
  currentInstant,
  Ids,
  type IdempotencyKey,
  type JobId,
  type JsonValue,
  type LeaseToken,
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
  type InvariantViolation,
  type MutationKind,
  type MutationOf,
  type QueueDraft,
  type QueueFile,
  type QueueFileError,
  type ResultOf,
} from "./model";

export class StorageFailure extends Data.TaggedError("StorageFailure")<{
  readonly message: string;
}> {}

export class QueueTooLarge extends Data.TaggedError("QueueTooLarge")<{
  readonly message: string;
}> {}

export class CompareAndSetConflict extends Data.TaggedError("CompareAndSetConflict")<{
  readonly message: string;
}> {}

export class CommitAttemptsExhausted extends Data.TaggedError("CommitAttemptsExhausted")<{
  readonly message: string;
}> {}

export class QueueBindingConflict extends Data.TaggedError("QueueBindingConflict")<{
  readonly message: string;
}> {}

type CommitError =
  | StorageFailure
  | QueueFileError
  | InvariantViolation
  | QueueTooLarge
  | CommitAttemptsExhausted;

export type BrokerError = CommitError | QueueBindingConflict;

export interface StoredQueue {
  readonly text: string;
  readonly etag: string;
}

// The R2 object that holds one queue. `put` writes only if the object still
// has `etag`, or does not exist yet when `etag` is none. It succeeds with the
// new etag, or with none when R2 holds a different version.
export class QueueStore extends Context.Service<
  QueueStore,
  {
    readonly get: (queue: QueueName) => Effect.Effect<Option.Option<StoredQueue>, StorageFailure>;
    readonly put: (
      queue: QueueName,
      body: string,
      etag: Option.Option<string>,
    ) => Effect.Effect<Option.Option<string>, StorageFailure>;
  }
>()("turbo/QueueStore") {
  static fromBucket(bucket: R2Bucket): Layer.Layer<QueueStore> {
    return Layer.succeed(QueueStore, {
      get: (queue) =>
        Effect.tryPromise({
          try: async () => {
            const object = await bucket.get(objectKey(queue));

            if (!object) return Option.none();

            return Option.some({ text: await object.text(), etag: object.etag });
          },
          catch: (error) =>
            new StorageFailure({ message: `R2 read for queue ${queue} failed: ${String(error)}` }),
        }),
      put: (queue, body, etag) =>
        Effect.tryPromise({
          try: async () => {
            const stored = await bucket.put(objectKey(queue), body, {
              onlyIf: Option.match(etag, {
                onNone: () => ({ etagDoesNotMatch: "*" }),
                onSome: (etagMatches) => ({ etagMatches }),
              }),
              httpMetadata: { contentType: "application/json" },
            });

            return Option.fromNullishOr(stored?.etag);
          },
          catch: (error) =>
            new StorageFailure({ message: `R2 write for queue ${queue} failed: ${String(error)}` }),
        }),
    });
  }
}

// Runs work that must finish after the request that started it has been
// answered. In a Durable Object that means `ctx.waitUntil`.
export class Background extends Context.Service<
  Background,
  { readonly run: (work: Effect.Effect<void>) => Effect.Effect<void> }
>()("turbo/Background") {
  static waitUntil(ctx: DurableObjectState): Layer.Layer<Background> {
    return Layer.succeed(Background, {
      run: (work) =>
        Effect.gen(function* () {
          const fiber = yield* Effect.forkDetach(work);

          ctx.waitUntil(Effect.runPromise(Fiber.await(fiber)));
        }),
    });
  }
}

interface Snapshot {
  readonly file: QueueFile;
  readonly etag: Option.Option<string>;
}

// A queued mutation, erased to a uniform type so different kinds can share one
// batch. `apply` runs the mutation against a draft and returns the effect that
// delivers its typed result; the result type never leaves the closure.
interface PendingCommand {
  readonly apply: (draft: QueueDraft) => Effect.Effect<void>;
  readonly fail: (cause: Cause.Cause<CommitError>) => Effect.Effect<void>;
}

interface FlushState {
  readonly pending: readonly PendingCommand[];
  // Completes when the running flush loop has drained `pending`. Absent when
  // no loop is running.
  readonly idle: Deferred.Deferred<void> | undefined;
}

const MAX_COMMIT_ATTEMPTS = 5;

export class Broker extends Context.Service<
  Broker,
  {
    readonly push: (
      queue: QueueName,
      payload: JsonValue,
      idempotencyKey: IdempotencyKey | undefined,
    ) => Effect.Effect<ResultOf<"push">, BrokerError>;
    readonly claim: (
      queue: QueueName,
      workerId: WorkerId,
      maxJobs: number,
    ) => Effect.Effect<ResultOf<"claim">, BrokerError>;
    readonly heartbeat: (
      queue: QueueName,
      jobId: JobId,
      leaseToken: LeaseToken,
    ) => Effect.Effect<ResultOf<"heartbeat">, BrokerError>;
    readonly complete: (
      queue: QueueName,
      jobId: JobId,
      leaseToken: LeaseToken,
    ) => Effect.Effect<ResultOf<"complete">, BrokerError>;
    readonly inspect: (queue: QueueName) => Effect.Effect<QueueFile, BrokerError>;
  }
>()("turbo/Broker") {
  static readonly layer = Layer.effect(
    Broker,
    Effect.gen(function* () {
      const settings = yield* Settings;
      const ids = yield* Ids;
      const store = yield* QueueStore;
      const background = yield* Background;
      const boundQueue = yield* Ref.make(Option.none<QueueName>());
      const snapshotRef = yield* Ref.make(Option.none<Snapshot>());
      const flush = yield* Ref.make<FlushState>({ pending: [], idle: undefined });

      const bindQueue = Effect.fn(function* (queue: QueueName) {
        const bound = yield* Ref.modify(boundQueue, (current) =>
          Option.isSome(current) && current.value !== queue
            ? [false, current]
            : [true, Option.some(queue)],
        );

        if (!bound) {
          return yield* new QueueBindingConflict({
            message: "Durable Object received operations for two queue names",
          });
        }
      });

      const load = Effect.fn(function* (queue: QueueName, force: boolean) {
        const cached = yield* Ref.get(snapshotRef);

        if (!force && Option.isSome(cached)) return cached.value;

        const object = yield* store.get(queue);

        const snapshot: Snapshot = Option.isSome(object)
          ? {
              file: yield* parseQueueFile(object.value.text, queue),
              etag: Option.some(object.value.etag),
            }
          : { file: emptyQueue(queue, yield* currentInstant), etag: Option.none() };

        yield* Ref.set(snapshotRef, Option.some(snapshot));

        return snapshot;
      });

      // Applies the batch to the latest snapshot and writes it with an R2
      // compare-and-set. A failed compare-and-set clears the snapshot, so the
      // next attempt reloads from R2.
      const commitOnce = Effect.fn(function* (queue: QueueName, batch: readonly PendingCommand[]) {
        const snapshot = yield* load(queue, false);
        const draft = openDraft(snapshot.file, ids.newLeaseToken);
        const deliveries = batch.map((command) => command.apply(draft));
        const { file, changed } = yield* closeDraft(draft);

        if (!changed) return deliveries;

        const body = JSON.stringify(file);

        if (new TextEncoder().encode(body).byteLength > settings.maxQueueBytes) {
          return yield* new QueueTooLarge({
            message: `queue exceeds configured MAX_QUEUE_BYTES of ${settings.maxQueueBytes}`,
          });
        }

        const stored = yield* store.put(queue, body, snapshot.etag);

        if (Option.isNone(stored)) {
          yield* Ref.set(snapshotRef, Option.none());

          return yield* new CompareAndSetConflict({
            message: `R2 object for queue ${queue} changed during the commit`,
          });
        }

        yield* Ref.set(snapshotRef, Option.some({ file, etag: stored }));

        return deliveries;
      });

      // Results are delivered only after R2 accepts the write.
      const commit = (queue: QueueName, batch: readonly PendingCommand[]) =>
        commitOnce(queue, batch).pipe(
          Effect.retry({
            times: MAX_COMMIT_ATTEMPTS - 1,
            while: Predicate.isTagged("CompareAndSetConflict"),
          }),
          Effect.catchTag("CompareAndSetConflict", () =>
            Effect.fail(
              new CommitAttemptsExhausted({
                message: `R2 compare-and-set failed after ${MAX_COMMIT_ATTEMPTS} attempts`,
              }),
            ),
          ),
        );

      const flushLoop = (idle: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          yield* Effect.sleep(settings.batchWindowMs);

          while (true) {
            // Taking the batch and ending the loop are one step, so a command
            // enqueued after the last batch always starts a new loop.
            const batch = yield* Ref.modify(
              flush,
              (state): [readonly PendingCommand[], FlushState] =>
                state.pending.length === 0
                  ? [state.pending, { pending: [], idle: undefined }]
                  : [state.pending, { pending: [], idle: state.idle }],
            );

            if (batch.length === 0) return;

            const queue = yield* Ref.get(boundQueue);

            if (Option.isNone(queue)) return yield* Effect.die("queue broker is not initialized");

            yield* commit(queue.value, batch).pipe(
              Effect.matchCauseEffect({
                onSuccess: (deliveries) => Effect.all(deliveries, { discard: true }),
                onFailure: (cause) =>
                  Effect.forEach(batch, (command) => command.fail(cause), { discard: true }),
              }),
            );
          }
        }).pipe(Effect.ensuring(Deferred.succeed(idle, undefined)));

      const enqueue = Effect.fn(function* <K extends MutationKind>(
        queue: QueueName,
        mutation: MutationOf<K>,
      ) {
        yield* bindQueue(queue);

        const result = yield* Deferred.make<ResultOf<K>, CommitError>();
        const idle = yield* Deferred.make<void>();

        const command: PendingCommand = {
          apply: (draft) => {
            const outcome = applyMutation(draft, mutation);

            return Deferred.succeed(result, outcome).pipe(Effect.asVoid);
          },
          fail: (cause) => Deferred.failCause(result, cause).pipe(Effect.asVoid),
        };

        const start = yield* Ref.modify(flush, (state): [boolean, FlushState] =>
          state.idle === undefined
            ? [true, { pending: [command], idle }]
            : [false, { pending: [...state.pending, command], idle: state.idle }],
        );

        if (start) yield* background.run(flushLoop(idle));

        return yield* Deferred.await(result);
      });

      return {
        push: Effect.fn(function* (queue, payload, idempotencyKey) {
          return yield* enqueue(queue, {
            kind: "push",
            id: ids.newJobId(),
            idempotencyKey,
            payload,
            now: yield* currentInstant,
          });
        }),

        claim: Effect.fn(function* (queue, workerId, maxJobs) {
          return yield* enqueue(queue, {
            kind: "claim",
            workerId,
            maxJobs,
            leaseSeconds: settings.leaseSeconds,
            now: yield* currentInstant,
          });
        }),

        heartbeat: Effect.fn(function* (queue, jobId, leaseToken) {
          return yield* enqueue(queue, {
            kind: "heartbeat",
            jobId,
            leaseToken,
            leaseSeconds: settings.leaseSeconds,
            now: yield* currentInstant,
          });
        }),

        complete: Effect.fn(function* (queue, jobId, leaseToken) {
          return yield* enqueue(queue, {
            kind: "complete",
            jobId,
            leaseToken,
            now: yield* currentInstant,
          });
        }),

        inspect: Effect.fn(function* (queue) {
          yield* bindQueue(queue);

          const { idle } = yield* Ref.get(flush);

          if (idle) yield* Deferred.await(idle);

          const snapshot = yield* load(queue, true);

          return snapshot.file;
        }),
      };
    }),
  );
}

// The Durable Object is only the RPC surface. Each method runs the `Broker`
// service on a runtime that lives as long as the object.
export class QueueBroker extends DurableObject<Env> {
  private readonly runtime: ManagedRuntime.ManagedRuntime<Broker, never>;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.runtime = ManagedRuntime.make(
      Broker.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Settings.fromEnv(env),
            Ids.layer,
            QueueStore.fromBucket(env.QUEUE_BUCKET),
            Background.waitUntil(ctx),
          ),
        ),
      ),
    );
  }

  push(
    queue: QueueName,
    payload: Encoded<JsonValue>,
    idempotencyKey: IdempotencyKey | undefined,
  ): Promise<Encoded<ResultOf<"push">>> {
    return this.run(Broker.use((broker) => broker.push(queue, decode(payload), idempotencyKey)));
  }

  claim(
    queue: QueueName,
    workerId: WorkerId,
    maxJobs: number,
  ): Promise<Encoded<ResultOf<"claim">>> {
    return this.run(Broker.use((broker) => broker.claim(queue, workerId, maxJobs)));
  }

  heartbeat(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"heartbeat">>> {
    return this.run(Broker.use((broker) => broker.heartbeat(queue, jobId, leaseToken)));
  }

  complete(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"complete">>> {
    return this.run(Broker.use((broker) => broker.complete(queue, jobId, leaseToken)));
  }

  inspect(queue: QueueName): Promise<Encoded<QueueFile>> {
    return this.run(Broker.use((broker) => broker.inspect(queue)));
  }

  private run<A>(effect: Effect.Effect<A, BrokerError, Broker>): Promise<Encoded<A>> {
    return this.runtime.runPromise(effect.pipe(Effect.map(encode)));
  }
}

function objectKey(queue: QueueName): string {
  return `queues/${encodeURIComponent(queue)}/queue.json`;
}
