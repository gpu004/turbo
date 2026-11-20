import {
  Cause,
  Context,
  Data,
  Effect,
  Layer,
  Logger,
  Schema,
  SchemaIssue,
  SchemaTransformation,
  Stream,
} from "effect";
import { QueueBroker } from "./broker";
import { Settings } from "./config";
import {
  IdempotencyKey,
  JobId,
  JsonText,
  JsonValue,
  LeaseToken,
  QueueName,
  WorkerId,
} from "./domain";
import type { MutationResult, PublicQueueFile } from "./model";
import { route, type Route } from "./router";
import { decode, encode, type Encoded } from "./rpc";

export { QueueBroker };

const requestObjectError = "request body must be a JSON object";

const maxJobsError = "maxJobs must be an integer from 1 to 100";

const PushBodySchema = Schema.Struct({
  payload: JsonValue.pipe(Schema.annotateKey({ messageMissingKey: "payload is required" })),
  idempotencyKey: Schema.optionalKey(
    Schema.String.pipe(Schema.catchDecoding(() => Effect.succeedNone)),
  ),
}).annotate({ message: requestObjectError });

const ClaimBodySchema = Schema.Struct({
  workerId: stringField("workerId", 200, WorkerId),
  maxJobs: Schema.Number.annotate({ message: maxJobsError }).pipe(
    Schema.check(
      Schema.isInt({ message: maxJobsError }),
      Schema.isBetween({ minimum: 1, maximum: 100 }, { message: maxJobsError }),
    ),
    Schema.withDecodingDefaultKey(Effect.succeed(1)),
  ),
}).annotate({ message: requestObjectError });

const LeaseBodySchema = Schema.Struct({
  leaseToken: stringField("leaseToken", 200, LeaseToken),
}).annotate({ message: requestObjectError });

// A failure the client caused. It becomes a JSON error response; anything
// else becomes a logged 500.
class HttpError extends Data.TaggedError("HttpError")<{
  readonly status: number;
  readonly code: string;
  readonly message: string;
}> {}

class BrokerCallFailed extends Data.TaggedError("BrokerCallFailed")<{
  readonly message: string;
}> {}

class BodyReadFailed extends Data.TaggedError("BodyReadFailed")<{
  readonly message: string;
}> {}

type BrokerStub = DurableObjectStub<QueueBroker>;

class Brokers extends Context.Service<
  Brokers,
  { readonly forQueue: (queue: QueueName) => BrokerStub }
>()("turbo/Brokers") {
  static fromNamespace(namespace: DurableObjectNamespace<QueueBroker>): Layer.Layer<Brokers> {
    return Layer.succeed(Brokers, { forQueue: (queue) => namespace.getByName(queue) });
  }
}

type RequestError = HttpError | BrokerCallFailed | BodyReadFailed;

const routes: ReadonlyArray<Route<Request, RequestError, Settings | Brokers>> = [
  route("GET", "/health", () => Effect.succeed(json({ ok: true }))),

  route("GET", "/queues/:queue", (_request, params) =>
    Effect.gen(function* () {
      const queue = yield* pathParam(QueueName, params.queue, "invalid_queue");

      return jsonText(yield* callBroker(queue, (broker) => broker.inspect(queue)));
    }),
  ),

  route("POST", "/queues/:queue/jobs", (request, params) =>
    Effect.gen(function* () {
      const queue = yield* pathParam(QueueName, params.queue, "invalid_queue");
      const input = yield* readJson(request, PushBodySchema);
      const rawKey = request.headers.get("idempotency-key")?.trim() || input.idempotencyKey?.trim();
      const idempotencyKey = rawKey ? yield* parseIdempotencyKey(rawKey) : undefined;

      const result = decode(
        yield* callBroker(queue, (broker) =>
          broker.push(queue, encode(input.payload), idempotencyKey),
        ),
      );

      if (!result.ok) {
        return json({ error: { code: result.code, message: result.message } }, 507);
      }

      return json(result, result.duplicate ? 200 : 201);
    }),
  ),

  route("POST", "/queues/:queue/claims", (request, params) =>
    Effect.gen(function* () {
      const queue = yield* pathParam(QueueName, params.queue, "invalid_queue");
      const input = yield* readJson(request, ClaimBodySchema);

      return jsonText(
        yield* callBroker(queue, (broker) => broker.claim(queue, input.workerId, input.maxJobs)),
      );
    }),
  ),

  route("POST", "/queues/:queue/jobs/:jobId/heartbeat", (request, params) =>
    Effect.gen(function* () {
      const queue = yield* pathParam(QueueName, params.queue, "invalid_queue");
      const jobId = yield* pathParam(JobId, params.jobId, "invalid_job_id");
      const input = yield* readJson(request, LeaseBodySchema);

      return mutationResponse(
        decode(
          yield* callBroker(queue, (broker) => broker.heartbeat(queue, jobId, input.leaseToken)),
        ),
      );
    }),
  ),

  route("POST", "/queues/:queue/jobs/:jobId/complete", (request, params) =>
    Effect.gen(function* () {
      const queue = yield* pathParam(QueueName, params.queue, "invalid_queue");
      const jobId = yield* pathParam(JobId, params.jobId, "invalid_job_id");
      const input = yield* readJson(request, LeaseBodySchema);

      return mutationResponse(
        decode(
          yield* callBroker(queue, (broker) => broker.complete(queue, jobId, input.leaseToken)),
        ),
      );
    }),
  ),
];

const errorLogger = Logger.layer([Logger.withConsoleError(Logger.formatJson)]);

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return Effect.runPromise(
      handleRequest(request).pipe(
        Effect.provide(
          Layer.mergeAll(
            Settings.fromEnv(env),
            Brokers.fromNamespace(env.QUEUE_BROKER),
            errorLogger,
          ),
        ),
      ),
    );
  },
} satisfies ExportedHandler<Env>;

function handleRequest(request: Request): Effect.Effect<Response, never, Settings | Brokers> {
  const path = new URL(request.url).pathname;

  return Effect.gen(function* () {
    const segments = yield* parseSegments(path);

    for (const candidate of routes) {
      const handle = candidate.match(request.method, segments);

      if (handle) return yield* handle(request);
    }

    return yield* new HttpError({ status: 404, code: "not_found", message: "route not found" });
  }).pipe(
    Effect.catchTag("HttpError", (error) =>
      Effect.succeed(json({ error: { code: error.code, message: error.message } }, error.status)),
    ),
    Effect.catchCause((cause) => {
      const error = Cause.squash(cause);

      return Effect.logError("request_failed").pipe(
        Effect.annotateLogs({
          method: request.method,
          path,
          error: error instanceof Error ? error.message : String(error),
        }),
        Effect.as(
          json({ error: { code: "internal_error", message: "queue operation failed" } }, 500),
        ),
      );
    }),
  );
}

function callBroker<A>(
  queue: QueueName,
  call: (broker: BrokerStub) => Promise<A>,
): Effect.Effect<A, BrokerCallFailed, Brokers> {
  return Brokers.use((brokers) =>
    Effect.tryPromise({
      try: () => call(brokers.forQueue(queue)),
      catch: (error) =>
        new BrokerCallFailed({ message: error instanceof Error ? error.message : String(error) }),
    }),
  );
}

// The first issue's message, which is the one a client sees.
function firstIssueMessage(error: Schema.SchemaError): string | undefined {
  return SchemaIssue.makeFormatterStandardSchemaV1()(error.issue).issues[0]?.message;
}

function pathParam<A>(
  schema: Schema.Codec<A, string>,
  value: string,
  code: string,
): Effect.Effect<A, HttpError> {
  return Schema.decodeEffect(schema)(value).pipe(
    Effect.mapError(
      (error) =>
        new HttpError({
          status: 400,
          code,
          message: firstIssueMessage(error) ?? `${code} in path`,
        }),
    ),
  );
}

function parseIdempotencyKey(value: string): Effect.Effect<IdempotencyKey, HttpError> {
  return Schema.decodeEffect(IdempotencyKey)(value).pipe(
    Effect.mapError(
      () =>
        new HttpError({
          status: 400,
          code: "invalid_idempotency_key",
          message: "idempotency key is too long",
        }),
    ),
  );
}

function mutationResponse(result: MutationResult): Response {
  if (result.ok) return json(result);
  const status = result.code === "job_not_found" ? 404 : 409;

  return json({ error: { code: result.code, message: result.message } }, status);
}

function readJson<A>(
  request: Request,
  schema: Schema.Codec<A, JsonValue>,
): Effect.Effect<A, HttpError | BodyReadFailed, Settings> {
  return Effect.gen(function* () {
    const body = request.body;

    if (!body) {
      return yield* new HttpError({
        status: 400,
        code: "invalid_json",
        message: "JSON request body is required",
      });
    }

    const { maxRequestBytes: maxBytes } = yield* Settings;

    const tooLarge = new HttpError({
      status: 413,
      code: "body_too_large",
      message: `request body exceeds ${maxBytes} bytes`,
    });

    const declaredLength = Number.parseInt(request.headers.get("content-length") ?? "", 10);

    if (Number.isFinite(declaredLength) && declaredLength > maxBytes) return yield* tooLarge;

    const chunks: Uint8Array[] = [];
    let received = 0;

    // Failing the stream cancels the reader, so an oversized body is not read
    // to the end.
    yield* Stream.fromReadableStream({
      evaluate: () => body,
      onError: (error) => new BodyReadFailed({ message: String(error) }),
    }).pipe(
      Stream.runForEach((chunk) => {
        received += chunk.byteLength;

        if (received > maxBytes) return Effect.fail(tooLarge);

        chunks.push(chunk);

        return Effect.void;
      }),
    );

    const bytes = new Uint8Array(received);
    let offset = 0;

    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }

    const json = yield* Schema.decodeEffect(JsonText)(new TextDecoder().decode(bytes)).pipe(
      Effect.mapError(
        () =>
          new HttpError({
            status: 400,
            code: "invalid_json",
            message: "request body is not valid JSON",
          }),
      ),
    );

    return yield* Schema.decodeEffect(schema)(json).pipe(
      Effect.mapError(
        (error) =>
          new HttpError({
            status: 400,
            code: "invalid_body",
            message: firstIssueMessage(error) ?? "request body is invalid",
          }),
      ),
    );
  });
}

function parseSegments(pathname: string): Effect.Effect<string[], HttpError> {
  return Effect.try({
    try: () => pathname.split("/").filter(Boolean).map(decodeURIComponent),
    catch: () =>
      new HttpError({
        status: 400,
        code: "invalid_path",
        message: "path contains invalid percent encoding",
      }),
  });
}

// A string field that is trimmed and then decoded as `target`. Every failure,
// including a missing key, reports the same message.
function stringField<A>(key: string, maxLength: number, target: Schema.Codec<A, string>) {
  const message = `${key} must be a non-empty string no longer than ${maxLength} characters`;

  return Schema.String.annotate({ message }).pipe(
    Schema.check(Schema.isMaxLength(maxLength, { message })),
    Schema.decodeTo(
      Schema.String.check(Schema.isNonEmpty({ message })),
      SchemaTransformation.trim(),
    ),
    Schema.decodeTo(target),
    Schema.annotateKey({ messageMissingKey: message }),
  );
}

type JsonResponseBody =
  | MutationResult
  | { error: { code: string; message: string } }
  | { ok: true };

function json(value: JsonResponseBody, status = 200): Response {
  return Response.json(value, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

// Sends a broker response as-is. The response body is the JSON text of `T`.
function jsonText<T extends JsonResponseBody | PublicQueueFile>(
  value: Encoded<T>,
  status = 200,
): Response {
  return new Response(value, {
    status,
    headers: {
      "cache-control": "no-store",
      "content-type": "application/json; charset=utf-8",
    },
  });
}
