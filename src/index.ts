import { QueueBroker } from "./broker";
import { parseConfig, type Config } from "./config";
import {
  IdempotencyKey,
  JobId,
  JsonValueSchema,
  LeaseToken,
  parseJson,
  QueueName,
  WorkerId,
} from "./domain";
import type { MutationResult, QueueFile } from "./model";
import { route, type Route } from "./router";
import { decode, encode, type Encoded } from "./rpc";
import { z } from "zod";

export { QueueBroker };

const requestObjectError = "request body must be a JSON object";

const invalidPayloadError = "payload is required";

const PushBodySchema = z.object(
  {
    payload: JsonValueSchema,
    idempotencyKey: z.string().optional().catch(undefined),
  },
  { error: requestObjectError },
);

const ClaimBodySchema = z.object(
  {
    workerId: stringField("workerId", 200).pipe(WorkerId),
    maxJobs: z
      .number({ error: "maxJobs must be an integer from 1 to 100" })
      .int("maxJobs must be an integer from 1 to 100")
      .min(1, "maxJobs must be an integer from 1 to 100")
      .max(100, "maxJobs must be an integer from 1 to 100")
      .default(1),
  },
  { error: requestObjectError },
);

const LeaseBodySchema = z.object(
  { leaseToken: stringField("leaseToken", 200).pipe(LeaseToken) },
  { error: requestObjectError },
);

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

interface RequestContext {
  readonly request: Request;
  readonly env: Env;
  readonly config: Config;
}

const routes: ReadonlyArray<Route<RequestContext>> = [
  route("GET", "/health", async () => json({ ok: true })),

  route("GET", "/queues/:queue", async ({ env }, params) => {
    const queue = pathParam(QueueName, params.queue, "invalid_queue");

    return jsonText(await env.QUEUE_BROKER.getByName(queue).inspect(queue));
  }),

  route("POST", "/queues/:queue/jobs", async ({ request, env, config }, params) => {
    const queue = pathParam(QueueName, params.queue, "invalid_queue");
    const input = await readJson(request, config, PushBodySchema);
    const rawKey = request.headers.get("idempotency-key")?.trim() || input.idempotencyKey?.trim();
    const idempotencyKey = rawKey ? parseIdempotencyKey(rawKey) : undefined;

    const result = decode(
      await env.QUEUE_BROKER.getByName(queue).push(queue, encode(input.payload), idempotencyKey),
    );

    return json(result, result.duplicate ? 200 : 201);
  }),

  route("POST", "/queues/:queue/claims", async ({ request, env, config }, params) => {
    const queue = pathParam(QueueName, params.queue, "invalid_queue");
    const input = await readJson(request, config, ClaimBodySchema);

    return jsonText(
      await env.QUEUE_BROKER.getByName(queue).claim(queue, input.workerId, input.maxJobs),
    );
  }),

  route(
    "POST",
    "/queues/:queue/jobs/:jobId/heartbeat",
    async ({ request, env, config }, params) => {
      const queue = pathParam(QueueName, params.queue, "invalid_queue");
      const jobId = pathParam(JobId, params.jobId, "invalid_job_id");
      const input = await readJson(request, config, LeaseBodySchema);

      return mutationResponse(
        decode(await env.QUEUE_BROKER.getByName(queue).heartbeat(queue, jobId, input.leaseToken)),
      );
    },
  ),

  route("POST", "/queues/:queue/jobs/:jobId/complete", async ({ request, env, config }, params) => {
    const queue = pathParam(QueueName, params.queue, "invalid_queue");
    const jobId = pathParam(JobId, params.jobId, "invalid_job_id");
    const input = await readJson(request, config, LeaseBodySchema);

    return mutationResponse(
      decode(await env.QUEUE_BROKER.getByName(queue).complete(queue, jobId, input.leaseToken)),
    );
  }),
];

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const segments = parseSegments(new URL(request.url).pathname);
      const context: RequestContext = { request, env, config: parseConfig(env) };

      for (const candidate of routes) {
        const handle = candidate.match(request.method, segments);

        if (handle) return await handle(context);
      }

      throw new HttpError(404, "not_found", "route not found");
    } catch (error) {
      if (error instanceof HttpError) {
        return json({ error: { code: error.code, message: error.message } }, error.status);
      }

      console.error(
        JSON.stringify({
          level: "error",
          event: "request_failed",
          method: request.method,
          path: new URL(request.url).pathname,
          error: error instanceof Error ? error.message : String(error),
        }),
      );

      return json({ error: { code: "internal_error", message: "queue operation failed" } }, 500);
    }
  },
} satisfies ExportedHandler<Env>;

function pathParam<S extends z.ZodType>(schema: S, value: string, code: string): z.output<S> {
  const parsed = schema.safeParse(value);

  if (!parsed.success) {
    throw new HttpError(400, code, parsed.error.issues[0]?.message ?? `${code} in path`);
  }

  return parsed.data;
}

function parseIdempotencyKey(value: string): IdempotencyKey {
  const parsed = IdempotencyKey.safeParse(value);

  if (!parsed.success) {
    throw new HttpError(400, "invalid_idempotency_key", "idempotency key is too long");
  }

  return parsed.data;
}

function mutationResponse(result: MutationResult): Response {
  if (result.ok) return json(result);
  const status = result.code === "job_not_found" ? 404 : 409;

  return json({ error: { code: result.code, message: result.message } }, status);
}

async function readJson<T>(request: Request, config: Config, schema: z.ZodType<T>): Promise<T> {
  if (!request.body) throw new HttpError(400, "invalid_json", "JSON request body is required");

  const maxBytes = config.maxRequestBytes;
  const declaredLength = Number.parseInt(request.headers.get("content-length") ?? "", 10);

  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
    throw new HttpError(413, "body_too_large", `request body exceeds ${maxBytes} bytes`);
  }

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let received = 0;

  while (true) {
    const { done, value } = await reader.read();

    if (done) break;
    received += value.byteLength;

    if (received > maxBytes) {
      await reader.cancel();
      throw new HttpError(413, "body_too_large", `request body exceeds ${maxBytes} bytes`);
    }

    chunks.push(value);
  }

  const bytes = new Uint8Array(received);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  const json = parseJson(new TextDecoder().decode(bytes));

  if (!json.ok) throw new HttpError(400, "invalid_json", "request body is not valid JSON");

  const parsed = schema.safeParse(json.value);

  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    const message = issue?.path[0] === "payload" ? invalidPayloadError : issue?.message;

    throw new HttpError(400, "invalid_body", message ?? "request body is invalid");
  }

  return parsed.data;
}

function parseSegments(pathname: string): string[] {
  try {
    return pathname.split("/").filter(Boolean).map(decodeURIComponent);
  } catch {
    throw new HttpError(400, "invalid_path", "path contains invalid percent encoding");
  }
}

function stringField(key: string, maxLength: number): z.ZodString {
  const message = `${key} must be a non-empty string no longer than ${maxLength} characters`;

  return z.string({ error: message }).max(maxLength, message).trim().min(1, message);
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
function jsonText<T extends JsonResponseBody | QueueFile>(
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
