import { QueueBroker } from "./broker";
import type { MutationResult } from "./model";

export { QueueBroker };

const QUEUE_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const MAX_IDEMPOTENCY_KEY_LENGTH = 200;

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await route(request, env);
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

async function route(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  if (request.method === "GET" && url.pathname === "/health") {
    return json({ ok: true });
  }

  const segments = parseSegments(url.pathname);
  if (segments[0] !== "queues" || !segments[1]) {
    throw new HttpError(404, "not_found", "route not found");
  }

  const queue = segments[1];
  if (!QUEUE_NAME.test(queue)) {
    throw new HttpError(
      400,
      "invalid_queue",
      "queue name must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
    );
  }

  const broker = env.QUEUE_BROKER.getByName(queue);

  if (request.method === "GET" && segments.length === 2) {
    return jsonText(await broker.inspect(queue));
  }

  if (request.method === "POST" && segments[2] === "jobs" && segments.length === 3) {
    const input = await readJson(request, env);
    if (!hasOwn(input, "payload")) {
      throw new HttpError(400, "invalid_body", "payload is required");
    }
    const headerKey = request.headers.get("idempotency-key")?.trim();
    const bodyKey =
      typeof input.idempotencyKey === "string" ? input.idempotencyKey.trim() : undefined;
    const idempotencyKey = headerKey || bodyKey;
    if (idempotencyKey && idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
      throw new HttpError(400, "invalid_idempotency_key", "idempotency key is too long");
    }

    const result = parseBrokerResult<Extract<MutationResult, { kind: "push" }>>(
      await broker.push(queue, JSON.stringify(input.payload), idempotencyKey),
    );
    return json(result, result.duplicate ? 200 : 201);
  }

  if (request.method === "POST" && segments[2] === "claims" && segments.length === 3) {
    const input = await readJson(request, env);
    const workerId = requireString(input, "workerId", 200);
    const maxJobs = input.maxJobs === undefined ? 1 : requireInteger(input, "maxJobs", 1, 100);
    const result = parseBrokerResult<Extract<MutationResult, { kind: "claim" }>>(
      await broker.claim(queue, workerId, maxJobs),
    );
    return json(result);
  }

  const jobId = segments[3];
  if (
    request.method === "POST" &&
    segments[2] === "jobs" &&
    jobId &&
    (segments[4] === "heartbeat" || segments[4] === "complete") &&
    segments.length === 5
  ) {
    const input = await readJson(request, env);
    const leaseToken = requireString(input, "leaseToken", 200);
    const resultJson =
      segments[4] === "heartbeat"
        ? await broker.heartbeat(queue, jobId, leaseToken)
        : await broker.complete(queue, jobId, leaseToken);
    const result = parseBrokerResult<MutationResult>(resultJson);
    return mutationResponse(result);
  }

  throw new HttpError(404, "not_found", "route not found");
}

function mutationResponse(result: MutationResult): Response {
  if (result.ok) return json(result);
  const status = result.code === "job_not_found" ? 404 : 409;
  return json({ error: { code: result.code, message: result.message } }, status);
}

async function readJson(request: Request, env: Env): Promise<Record<string, unknown>> {
  if (!request.body) throw new HttpError(400, "invalid_json", "JSON request body is required");

  const maxBytes = boundedInteger(env.MAX_REQUEST_BYTES, 256 * 1024, 1_024, 4 * 1024 * 1024);
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

  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new HttpError(400, "invalid_json", "request body is not valid JSON");
  }

  if (!isRecord(value)) {
    throw new HttpError(400, "invalid_body", "request body must be a JSON object");
  }
  return value;
}

function parseSegments(pathname: string): string[] {
  try {
    return pathname.split("/").filter(Boolean).map(decodeURIComponent);
  } catch {
    throw new HttpError(400, "invalid_path", "path contains invalid percent encoding");
  }
}

function requireString(input: Record<string, unknown>, key: string, maxLength: number): string {
  const value = input[key];
  if (typeof value !== "string" || value.trim().length === 0 || value.length > maxLength) {
    throw new HttpError(
      400,
      "invalid_body",
      `${key} must be a non-empty string no longer than ${maxLength} characters`,
    );
  }
  return value.trim();
}

function requireInteger(
  input: Record<string, unknown>,
  key: string,
  minimum: number,
  maximum: number,
): number {
  const value = input[key];
  if (!Number.isInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    throw new HttpError(
      400,
      "invalid_body",
      `${key} must be an integer from ${minimum} to ${maximum}`,
    );
  }
  return value as number;
}

function boundedInteger(value: string, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
}

function hasOwn(input: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(input, key);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function json(value: unknown, status = 200): Response {
  return Response.json(value, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

function jsonText(value: string, status = 200): Response {
  return new Response(value, {
    status,
    headers: {
      "cache-control": "no-store",
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function parseBrokerResult<T extends MutationResult>(value: string): T {
  return JSON.parse(value) as T;
}
