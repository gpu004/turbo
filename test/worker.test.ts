import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { JsonValue } from "../src/model";

describe("queue Worker", () => {
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
    const firstJobId = first.job?.id;
    expect(firstJobId).toBeTypeOf("string");

    if (!firstJobId) throw new Error("push returned a response without a job id");

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
    expect(duplicate.job?.id).toBe(firstJobId);

    const claimResponse = await post("/queues/integration/claims", {
      workerId: "indexer-1",
      maxJobs: 1,
    });

    const claim = await claimResponse.json<ClaimResponse>();
    expect(claim.jobs).toHaveLength(1);
    expect(claim.jobs[0]?.id).toBe(firstJobId);
    const leaseToken = claim.jobs[0]?.lease?.token;
    expect(leaseToken).toBeTypeOf("string");

    if (!leaseToken) throw new Error("claim returned a job without a lease token");

    const inspectResponse = await exports.default.fetch("https://queue.test/queues/integration");
    const inspected = await inspectResponse.json<{ jobs: Array<{ lease?: { token?: string } }> }>();
    expect(inspected.jobs[0]?.lease).not.toHaveProperty("token");

    const wrongCompletion = await post(`/queues/integration/jobs/${firstJobId}/complete`, {
      leaseToken: "wrong-token",
    });

    expect(wrongCompletion.status).toBe(409);

    const heartbeat = await post(`/queues/integration/jobs/${firstJobId}/heartbeat`, {
      leaseToken,
    });

    expect(heartbeat.status).toBe(200);

    const completion = await post(`/queues/integration/jobs/${firstJobId}/complete`, {
      leaseToken,
    });

    expect(completion.status).toBe(200);

    const stateResponse = await exports.default.fetch("https://queue.test/queues/integration");
    const state = await stateResponse.json<{ jobs: unknown[]; revision: number }>();
    expect(state.jobs).toEqual([]);
    expect(state.revision).toBeGreaterThanOrEqual(4);
  });

  it("deduplicates completed jobs and rejects unknown completions", async () => {
    const initial = await post(
      "/queues/completed/jobs",
      { payload: { value: 1 } },
      { "idempotency-key": "key-1" },
    );

    const first = await initial.json<PushResponse>();
    const jobId = first.job?.id;
    expect(jobId).toBeTypeOf("string");

    if (!jobId) throw new Error("push returned a response without a job id");

    const claimResponse = await post("/queues/completed/claims", { workerId: "worker" });
    const claim = await claimResponse.json<ClaimResponse>();
    const leaseToken = claim.jobs[0]?.lease?.token;

    if (!leaseToken) throw new Error("claim returned a job without a lease token");
    await post(`/queues/completed/jobs/${jobId}/complete`, { leaseToken });

    const duplicateResponse = await post(
      "/queues/completed/jobs",
      { payload: { value: 1 } },
      { "idempotency-key": "key-1" },
    );

    expect(duplicateResponse.status).toBe(200);
    expect(await duplicateResponse.json()).toMatchObject({
      ok: true,
      kind: "push",
      duplicate: true,
      jobId,
    });

    const unknown = await post("/queues/completed/jobs/never-existed/complete", {
      leaseToken: "any-token",
    });

    expect(unknown.status).toBe(404);
    expect(await unknown.json()).toMatchObject({
      error: { code: "job_not_found" },
    });

    const stateResponse = await exports.default.fetch("https://queue.test/queues/completed");
    const state = await stateResponse.json<{ jobs: unknown[] }>();
    expect(state.jobs).toEqual([]);
  });

  it("redacts lease tokens from inspected queues and duplicate leased pushes", async () => {
    const initial = await post(
      "/queues/redacted/jobs",
      { payload: { value: 1 } },
      { "idempotency-key": "key-1" },
    );

    const first = await initial.json<PushResponse>();
    const jobId = first.job?.id;

    if (!jobId) throw new Error("push returned a response without a job id");

    const claimResponse = await post("/queues/redacted/claims", { workerId: "worker" });
    const claim = await claimResponse.json<ClaimResponse>();
    expect(claim.jobs[0]?.id).toBe(jobId);
    const leaseToken = claim.jobs[0]?.lease?.token;
    expect(leaseToken).toBeTypeOf("string");

    if (!leaseToken) throw new Error("claim returned a job without a lease token");

    const duplicateResponse = await post(
      "/queues/redacted/jobs",
      { payload: { value: 1 } },
      { "idempotency-key": "key-1" },
    );

    expect(duplicateResponse.status).toBe(200);
    const duplicate = await duplicateResponse.json<PushResponse>();
    expect(duplicate.job?.id).toBe(jobId);
    expect(duplicate.job).not.toHaveProperty("lease.token");

    const inspectResponse = await exports.default.fetch("https://queue.test/queues/redacted");

    const state = await inspectResponse.json<{
      jobs: Array<{ id: string; lease?: { token?: string } }>;
    }>();

    expect(state.jobs[0]?.id).toBe(jobId);
    expect(state.jobs[0]?.lease).not.toHaveProperty("token");
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

interface PushResponse {
  duplicate?: boolean;
  job?: { id: string; lease?: { token?: string } };
  jobId?: string;
}

interface ClaimResponse {
  jobs: Array<{ id: string; lease?: { token: string } }>;
}
