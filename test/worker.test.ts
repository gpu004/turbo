import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";

describe("queue Worker", () => {
  it("pushes, deduplicates, claims, heartbeats, and completes a job", async () => {
    const firstPush = await post("/queues/integration/jobs", {
      payload: { namespace: "42", walSequence: 1 },
    }, { "idempotency-key": "wal-1" });
    expect(firstPush.status).toBe(201);
    const first = await firstPush.json<PushResponse>();

    const duplicatePush = await post("/queues/integration/jobs", {
      payload: { namespace: "42", walSequence: 1 },
    }, { "idempotency-key": "wal-1" });
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

    const wrongCompletion = await post(
      `/queues/integration/jobs/${first.job.id}/complete`,
      { leaseToken: "wrong-token" },
    );
    expect(wrongCompletion.status).toBe(409);

    const heartbeat = await post(
      `/queues/integration/jobs/${first.job.id}/heartbeat`,
      { leaseToken },
    );
    expect(heartbeat.status).toBe(200);

    const completion = await post(
      `/queues/integration/jobs/${first.job.id}/complete`,
      { leaseToken },
    );
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
});

function post(path: string, body: unknown, headers: Record<string, string> = {}): Promise<Response> {
  return exports.default.fetch(`https://queue.test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

interface PushResponse {
  duplicate: boolean;
  job: { id: string };
}

interface ClaimResponse {
  jobs: Array<{ id: string; lease?: { token: string } }>;
}
