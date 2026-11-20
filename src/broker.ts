import { DurableObject } from "cloudflare:workers";
import {
  Cause,
  Clock,
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
  Schedule,
} from "effect";
import { Settings } from "./config";
import {
  currentInstant,
  Ids,
  type CommitId,
  type IdempotencyKey,
  type Instant,
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
  planShedding,
  publicQueueFile,
  serializedBytes,
  type InvariantViolation,
  type MutationKind,
  type MutationOf,
  type PublicQueueFile,
  type QueueDraft,
  type QueueFile,
  type QueueFileError,
  type ResultOf,
  type Shedding,
} from "./model";

// R2 ends an error message with its numeric code. These codes mean the same
// request may succeed later: too many requests (R2 takes about one write per
// second to a key), internal error, service unavailable, and client
// disconnected. https://developers.cloudflare.com/r2/api/error-codes/
const RETRYABLE_R2_CODE = /\((?:10058|10001|10043|10054)\)\s*$/u;

export class StorageFailure extends Data.TaggedError("StorageFailure")<{
  readonly message: string;
  readonly retryable: boolean;
}> {}

function storageFailure(action: string, queue: QueueName, detail: string): StorageFailure {
  return new StorageFailure({
    message: `R2 ${action} for queue ${queue} failed: ${detail}`,
    retryable: RETRYABLE_R2_CODE.test(detail),
  });
}

export class Overloaded extends Data.TaggedError("Overloaded")<{
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

export type BrokerError = CommitError | QueueBindingConflict | Overloaded;

// What the Durable Object returns instead of failing when the queue cannot
// take more work right now. The Worker answers it with 503 and Retry-After.
export interface Unavailable {
  readonly ok: false;
  readonly kind: "unavailable";
  readonly code: "overloaded" | "storage_unavailable";
  readonly message: string;
}

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
            storageFailure("read", queue, error instanceof Error ? error.message : String(error)),
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
            storageFailure("write", queue, error instanceof Error ? error.message : String(error)),
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
//
// `now` is the time of the commit attempt, not of the request. A command can
// wait a commit interval or more before it is applied, and a lease must start
// when it is written.
interface PendingCommand {
  readonly apply: (draft: QueueDraft, now: Instant) => Effect.Effect<void>;
  readonly fail: (cause: Cause.Cause<CommitError>) => Effect.Effect<void>;
  // Payload bytes this command holds in memory until it commits.
  readonly bytes: number;
}

interface FlushState {
  readonly pending: readonly PendingCommand[];
  readonly pendingBytes: number;
  // Completes when the running flush loop has drained `pending`. Absent when
  // no loop is running.
  readonly idle: Deferred.Deferred<void> | undefined;
}

const MAX_COMMIT_ATTEMPTS = 5;

type Retryable = CompareAndSetConflict | (StorageFailure & { readonly retryable: true });

function isRetryable(error: CommitError | CompareAndSetConflict): error is Retryable {
  return (
    Predicate.isTagged(error, "CompareAndSetConflict") ||
    (Predicate.isTagged(error, "StorageFailure") && error.retryable)
  );
}

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
    readonly inspect: (queue: QueueName) => Effect.Effect<PublicQueueFile, BrokerError>;
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
      const lastPutAt = yield* Ref.make(Option.none<number>());
      const flush = yield* Ref.make<FlushState>({ pending: [], pendingBytes: 0, idle: undefined });

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

      // Applies the batch to the snapshot until the result fits in
      // MAX_QUEUE_BYTES, giving up what `planShedding` picks each time it does
      // not. Every attempt starts again from the snapshot, so only the last
      // attempt's results are delivered.
      const applyWithinLimit = Effect.fn(function* (
        snapshot: Snapshot,
        batch: readonly PendingCommand[],
        commitId: CommitId,
      ) {
        const now = yield* currentInstant;
        let shed: Shedding | undefined;

        while (true) {
          const draft = openDraft(snapshot.file, ids.newLeaseToken, shed);
          const deliveries = batch.map((command) => command.apply(draft, now));
          const closed = yield* closeDraft(draft);
          const file = closed.changed ? { ...closed.file, commitId } : closed.file;
          const overflow = serializedBytes(file) - settings.maxQueueBytes;

          if (!closed.changed || overflow <= 0)
            return { file, changed: closed.changed, deliveries };

          shed = planShedding(snapshot.file, closed.file, draft.shed, overflow);

          if (!shed) {
            return yield* new QueueTooLarge({
              message: `queue exceeds configured MAX_QUEUE_BYTES of ${settings.maxQueueBytes}`,
            });
          }
        }
      });

      // R2 takes about one write per second to the same key. Waits until
      // QUEUE_COMMIT_INTERVAL_MS has passed since the last write started.
      const waitForTurn = Effect.gen(function* () {
        const last = yield* Ref.get(lastPutAt);

        if (Option.isNone(last)) return;

        const elapsed = (yield* Clock.currentTimeMillis) - last.value;

        if (elapsed < settings.commitIntervalMs) {
          yield* Effect.sleep(settings.commitIntervalMs - elapsed);
        }
      });

      const pacedPut = Effect.fn(function* (
        queue: QueueName,
        body: string,
        etag: Option.Option<string>,
      ) {
        yield* waitForTurn;
        yield* Ref.set(lastPutAt, Option.some(yield* Clock.currentTimeMillis));

        return yield* store.put(queue, body, etag);
      });

      // Applies the batch to the latest snapshot and writes it with an R2
      // compare-and-set. A failed compare-and-set clears the snapshot, so the
      // next attempt reloads from R2.
      //
      // A write that fails with an error may still have landed. `unconfirmed`
      // keeps the deliveries of each such write by its commit id. If the
      // reloaded file carries one of those ids, that write landed, and its
      // results are delivered instead of applying the batch a second time.
      const commitOnce = Effect.fn(function* (
        queue: QueueName,
        batch: readonly PendingCommand[],
        unconfirmed: Map<CommitId, readonly Effect.Effect<void>[]>,
      ) {
        const snapshot = yield* load(queue, false);
        const landed = snapshot.file.commitId && unconfirmed.get(snapshot.file.commitId);

        if (landed) return landed;

        const commitId = ids.newCommitId();
        const { file, changed, deliveries } = yield* applyWithinLimit(snapshot, batch, commitId);

        if (!changed) return deliveries;

        const body = JSON.stringify(file);

        const stored = yield* pacedPut(queue, body, snapshot.etag).pipe(
          Effect.tapError(() =>
            Effect.sync(() => unconfirmed.set(commitId, deliveries)).pipe(
              Effect.andThen(Ref.set(snapshotRef, Option.none())),
            ),
          ),
        );

        if (Option.isNone(stored)) {
          yield* Ref.set(snapshotRef, Option.none());

          return yield* new CompareAndSetConflict({
            message: `R2 object for queue ${queue} changed during the commit`,
          });
        }

        yield* Ref.set(snapshotRef, Option.some({ file, etag: stored }));

        return deliveries;
      });

      // Results are delivered only after R2 accepts the write. A lost
      // compare-and-set and a retryable R2 error are retried with jittered
      // backoff, on top of the pacing in `pacedPut`.
      const commit = (queue: QueueName, batch: readonly PendingCommand[]) =>
        commitOnce(queue, batch, new Map()).pipe(
          Effect.retry({
            times: MAX_COMMIT_ATTEMPTS - 1,
            while: isRetryable,
            schedule: Schedule.exponential("100 millis").pipe(Schedule.jittered),
          }),
          Effect.catchIf(isRetryable, (error) =>
            Effect.fail(
              new CommitAttemptsExhausted({
                message: `queue commit failed after ${MAX_COMMIT_ATTEMPTS} attempts: ${error.message}`,
              }),
            ),
          ),
        );

      const flushLoop = (idle: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          yield* Effect.sleep(settings.batchWindowMs);

          while (true) {
            // Commands that arrive while the broker waits for its next write
            // join the batch.
            yield* waitForTurn;

            // Taking the batch and ending the loop are one step, so a command
            // enqueued after the last batch always starts a new loop.
            const batch = yield* Ref.modify(
              flush,
              (state): [readonly PendingCommand[], FlushState] =>
                state.pending.length === 0
                  ? [state.pending, { pending: [], pendingBytes: 0, idle: undefined }]
                  : [state.pending, { pending: [], pendingBytes: 0, idle: state.idle }],
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
        mutation: (now: Instant) => MutationOf<K>,
        bytes = 0,
      ) {
        yield* bindQueue(queue);

        const result = yield* Deferred.make<ResultOf<K>, CommitError>();
        const idle = yield* Deferred.make<void>();

        const command: PendingCommand = {
          apply: (draft, now) => {
            const outcome = applyMutation(draft, mutation(now));

            return Deferred.succeed(result, outcome).pipe(Effect.asVoid);
          },
          fail: (cause) => Deferred.failCause(result, cause).pipe(Effect.asVoid),
          bytes,
        };

        // Waiting payloads beyond MAX_QUEUE_BYTES could not fit in one commit
        // anyway, and bounding them keeps a burst of large pushes from
        // exhausting isolate memory.
        const queued = yield* Ref.modify(
          flush,
          (state): ["start" | "joined" | "full", FlushState] => {
            const full =
              state.pending.length >= settings.maxPendingMutations ||
              (state.pending.length > 0 && state.pendingBytes + bytes > settings.maxQueueBytes);

            if (full) return ["full", state];

            const pending = [...state.pending, command];
            const pendingBytes = state.pendingBytes + bytes;

            return state.idle === undefined
              ? ["start", { pending, pendingBytes, idle }]
              : ["joined", { pending, pendingBytes, idle: state.idle }];
          },
        );

        if (queued === "full") {
          return yield* new Overloaded({
            message: "too many mutations are waiting for the next commit",
          });
        }

        if (queued === "start") yield* background.run(flushLoop(idle));

        return yield* Deferred.await(result);
      });

      return {
        push: Effect.fn(function* (queue, payload, idempotencyKey) {
          // One id for every attempt, so shedding and recovery see one job.
          const id = ids.newJobId();

          return yield* enqueue(
            queue,
            (now) => ({ kind: "push", id, idempotencyKey, payload, now }),
            serializedBytes(payload),
          );
        }),

        claim: Effect.fn(function* (queue, workerId, maxJobs) {
          return yield* enqueue(queue, (now) => ({
            kind: "claim",
            workerId,
            maxJobs,
            leaseSeconds: settings.leaseSeconds,
            now,
          }));
        }),

        heartbeat: Effect.fn(function* (queue, jobId, leaseToken) {
          return yield* enqueue(queue, (now) => ({
            kind: "heartbeat",
            jobId,
            leaseToken,
            leaseSeconds: settings.leaseSeconds,
            now,
          }));
        }),

        complete: Effect.fn(function* (queue, jobId, leaseToken) {
          return yield* enqueue(queue, (now) => ({
            kind: "complete",
            jobId,
            leaseToken,
            retainSeconds: settings.completedRetentionSeconds,
            now,
          }));
        }),

        inspect: Effect.fn(function* (queue) {
          yield* bindQueue(queue);

          const { idle } = yield* Ref.get(flush);

          if (idle) yield* Deferred.await(idle);

          const snapshot = yield* load(queue, true);

          return publicQueueFile(snapshot.file);
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
  ): Promise<Encoded<ResultOf<"push"> | Unavailable>> {
    return this.mutate(Broker.use((broker) => broker.push(queue, decode(payload), idempotencyKey)));
  }

  claim(
    queue: QueueName,
    workerId: WorkerId,
    maxJobs: number,
  ): Promise<Encoded<ResultOf<"claim"> | Unavailable>> {
    return this.mutate(Broker.use((broker) => broker.claim(queue, workerId, maxJobs)));
  }

  heartbeat(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"heartbeat"> | Unavailable>> {
    return this.mutate(Broker.use((broker) => broker.heartbeat(queue, jobId, leaseToken)));
  }

  complete(
    queue: QueueName,
    jobId: JobId,
    leaseToken: LeaseToken,
  ): Promise<Encoded<ResultOf<"complete"> | Unavailable>> {
    return this.mutate(Broker.use((broker) => broker.complete(queue, jobId, leaseToken)));
  }

  inspect(queue: QueueName): Promise<Encoded<PublicQueueFile>> {
    return this.run(Broker.use((broker) => broker.inspect(queue)));
  }

  // Errors cannot cross the RPC boundary with their type, so the two the
  // client should retry later travel as an `Unavailable` result.
  private mutate<A>(
    effect: Effect.Effect<A, BrokerError, Broker>,
  ): Promise<Encoded<A | Unavailable>> {
    const unavailable = (code: Unavailable["code"], message: string): Unavailable => ({
      ok: false,
      kind: "unavailable",
      code,
      message,
    });

    return this.run(
      effect.pipe(
        Effect.catchTags({
          Overloaded: (error) => Effect.succeed(unavailable("overloaded", error.message)),
          CommitAttemptsExhausted: (error) =>
            Effect.succeed(unavailable("storage_unavailable", error.message)),
        }),
      ),
    );
  }

  private run<A>(effect: Effect.Effect<A, BrokerError, Broker>): Promise<Encoded<A>> {
    return this.runtime.runPromise(effect.pipe(Effect.map(encode)));
  }
}

function objectKey(queue: QueueName): string {
  return `queues/${encodeURIComponent(queue)}/queue.json`;
}
