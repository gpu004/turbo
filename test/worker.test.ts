import { env, exports } from "cloudflare:workers";
import { beforeAll, describe, expect, it } from "vitest";
import type { JsonValue } from "../src/domain";

describe("queue Worker", () => {
  beforeAll(async () => {
    const warmup = await exports.default.fetch("https://queue.test/queues/warmup");
    expect(warmup.status).toBe(200);
  }, 30_000);

  it("pushes, deduplicates, claims, heartbeats, and completes a job", async () => {
    const firstPush = await post(
      "/queues/integration/jobs",
      {
        payload: { namespace: "42", walSequence: 1 },
      },
      { "idempotency-key": "wal-1" },
    );

    expect(firstPush.status).toBe(201);
    const first = await firstPush.json<PushResponse>();

    const duplicatePush = await post(
      "/queues/integration/jobs",
      {
        payload: { namespace: "42", walSequence: 1 },
      },
      { "idempotency-key": "wal-1" },
    );

    expect(duplicatePush.status).toBe(200);
    const duplicate = await duplicatePush.json<PushResponse>();
    expect(duplicate.duplicate).toBe(true);
    expect(duplicate.job.id).toBe(first.job.id);

    const claimResponse = await post("/queues/integration/claims", {
      workerId: "indexer-1",
      maxJobs: 1,
    });

    const claim = await claimResponse.json<ClaimResponse>();
    expect(claim.jobs).toHaveLength(1);
    expect(claim.jobs[0]?.id).toBe(first.job.id);
    const leaseToken = claim.jobs[0]?.lease?.token;
    expect(leaseToken).toBeTypeOf("string");

    if (!leaseToken) throw new Error("claim returned a job without a lease token");

    const wrongCompletion = await post(`/queues/integration/jobs/${first.job.id}/complete`, {
      leaseToken: "wrong-token",
    });

    expect(wrongCompletion.status).toBe(409);

    const heartbeat = await post(`/queues/integration/jobs/${first.job.id}/heartbeat`, {
      leaseToken,
    });

    expect(heartbeat.status).toBe(200);

    const completion = await post(`/queues/integration/jobs/${first.job.id}/complete`, {
      leaseToken,
    });

    expect(completion.status).toBe(200);

    const stateResponse = await exports.default.fetch("https://queue.test/queues/integration");
    const state = await stateResponse.json<{ jobs: unknown[]; revision: number }>();
    expect(state.jobs).toEqual([]);
    expect(state.revision).toBeGreaterThanOrEqual(4);
  });

  it("claims concurrent jobs only once", async () => {
    await Promise.all([
      post("/queues/concurrent/jobs", { payload: { n: 1 } }),
      post("/queues/concurrent/jobs", { payload: { n: 2 } }),
    ]);

    const queuedResponse = await exports.default.fetch("https://queue.test/queues/concurrent");
    const queued = await queuedResponse.json<{ jobs: unknown[]; revision: number }>();
    expect(queued.jobs).toHaveLength(2);
    expect(queued.revision).toBe(1);

    const [one, two] = await Promise.all([
      post("/queues/concurrent/claims", { workerId: "a" }),
      post("/queues/concurrent/claims", { workerId: "b" }),
    ]);

    const first = await one.json<ClaimResponse>();
    const second = await two.json<ClaimResponse>();
    expect(first.jobs).toHaveLength(1);
    expect(second.jobs).toHaveLength(1);
    expect(first.jobs[0]?.id).not.toBe(second.jobs[0]?.id);
  });

  it("keeps a completed job's idempotency key and fences repeat completions", async () => {
    const pushed = await post("/queues/retained/jobs", { payload: 1 }, { "idempotency-key": "k" });
    const { job } = await pushed.json<PushResponse>();
    const leaseToken = await claimOne("retained");

    const completion = await post(`/queues/retained/jobs/${job.id}/complete`, { leaseToken });
    expect(completion.status).toBe(200);

    const retry = await post("/queues/retained/jobs", { payload: 1 }, { "idempotency-key": "k" });
    expect(retry.status).toBe(200);
    await expect(retry.json()).resolves.toMatchObject({ duplicate: true, jobId: job.id });

    const repeat = await post(`/queues/retained/jobs/${job.id}/complete`, { leaseToken });
    expect(repeat.status).toBe(200);
    await expect(repeat.json()).resolves.toMatchObject({ alreadyCompleted: true });

    const forged = await post(`/queues/retained/jobs/${job.id}/complete`, {
      leaseToken: "forged",
    });

    expect(forged.status).toBe(409);

    const unknown = await post("/queues/retained/jobs/never-existed/complete", { leaseToken });
    expect(unknown.status).toBe(404);
    await expect(unknown.json()).resolves.toMatchObject({ error: { code: "job_not_found" } });
  });

  it("returns a lease token only to the worker that holds it", async () => {
    await post("/queues/redacted/jobs", { payload: 1 }, { "idempotency-key": "leased" });
    const done = await post("/queues/redacted/jobs", { payload: 2 });
    const doneId = (await done.json<PushResponse>()).job.id;

    const leasedToken = await claimOne("redacted");
    const doneToken = await claimOne("redacted");
    await post(`/queues/redacted/jobs/${doneId}/complete`, { leaseToken: doneToken });

    const duplicate = await post(
      "/queues/redacted/jobs",
      { payload: 1 },
      { "idempotency-key": "leased" },
    );

    const inspected = await exports.default.fetch("https://queue.test/queues/redacted");
    const text = `${await duplicate.text()}${await inspected.text()}`;

    expect(text).toContain("workerId");
    expect(text).not.toContain(leasedToken);
    expect(text).not.toContain(doneToken);
  });

  it("validates request bodies at the JSON boundary", async () => {
    const missingPayload = await post("/queues/validation/jobs", {});
    expect(missingPayload.status).toBe(400);
    await expect(missingPayload.json()).resolves.toMatchObject({
      error: { code: "invalid_body", message: "payload is required" },
    });

    const nullPayload = await post("/queues/validation/jobs", { payload: null });
    expect(nullPayload.status).toBe(201);

    const invalidMaxJobs = await post("/queues/validation/claims", {
      workerId: "worker",
      maxJobs: null,
    });

    expect(invalidMaxJobs.status).toBe(400);
    await expect(invalidMaxJobs.json()).resolves.toMatchObject({
      error: { code: "invalid_body", message: "maxJobs must be an integer from 1 to 100" },
    });
  });
});

describe("queue Worker failures", () => {
  it("answers 500 without details when the stored queue is corrupt", async () => {
    await env.QUEUE_BUCKET.put("queues/corrupt/queue.json", "not json");

    const response = await exports.default.fetch("https://queue.test/queues/corrupt");

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toEqual({
      error: { code: "internal_error", message: "queue operation failed" },
    });
  });
});

function post(
  path: string,
  body: JsonValue,
  headers: Record<string, string> = {},
): Promise<Response> {
  return exports.default.fetch(`https://queue.test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

async function claimOne(queue: string): Promise<string> {
  const response = await post(`/queues/${queue}/claims`, { workerId: "worker", maxJobs: 1 });
  const token = (await response.json<ClaimResponse>()).jobs[0]?.lease?.token;

  if (!token) throw new Error(`claim on ${queue} returned no lease token`);

  return token;
}

interface PushResponse {
  duplicate: boolean;
  job: { id: string };
}

interface ClaimResponse {
  jobs: Array<{ id: string; lease?: { token: string } }>;
}
