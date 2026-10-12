import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { JsonValue } from "../src/model";

describe("queue size limit", () => {
  it("rejects an oversized push without failing a concurrent completion", async () => {
    const initial = await post("/queues/limited/jobs", { payload: "small" });
    expect(initial.status).toBe(201);

    const claimResponse = await post("/queues/limited/claims", { workerId: "worker" });
    const claim = await claimResponse.json<ClaimResponse>();
    const job = claim.jobs[0];

    if (!job) throw new Error("claim returned no job");

    const leaseToken = job.lease?.token;

    if (!leaseToken) throw new Error("claim returned a job without a lease token");

    const [completion, oversizedPush] = await Promise.all([
      post(`/queues/limited/jobs/${job.id}/complete`, { leaseToken }),
      post("/queues/limited/jobs", { payload: "x".repeat(2_000) }),
    ]);

    expect(completion.status).toBe(200);
    expect(oversizedPush.status).toBe(507);
    expect(await oversizedPush.json()).toMatchObject({
      error: { code: "queue_full" },
    });

    const stateResponse = await exports.default.fetch("https://queue.test/queues/limited");
    const state = await stateResponse.json<QueueState>();
    expect(state.jobs).toEqual([]);
    expect(state.completed).toHaveLength(1);
    expect(state.completed[0]?.jobId).toBe(job?.id);
  });

  it("evicts the oldest completion tombstone to make room for a claim", async () => {
    const firstPush = await post("/queues/tombstone-pressure/jobs", { payload: "first" });
    expect(firstPush.status).toBe(201);

    const firstClaimResponse = await post("/queues/tombstone-pressure/claims", {
      workerId: "worker",
    });

    const firstClaim = await firstClaimResponse.json<ClaimResponse>();
    const firstJob = firstClaim.jobs[0];

    if (!firstJob) throw new Error("claim returned no job");

    const firstLeaseToken = firstJob.lease?.token;

    if (!firstLeaseToken) throw new Error("claim returned a job without a lease token");

    const firstCompletion = await post(`/queues/tombstone-pressure/jobs/${firstJob.id}/complete`, {
      leaseToken: firstLeaseToken,
    });

    expect(firstCompletion.status).toBe(200);

    await new Promise((resolve) => setTimeout(resolve, 10));

    const secondPush = await post("/queues/tombstone-pressure/jobs", { payload: "second" });
    expect(secondPush.status).toBe(201);

    const secondClaimResponse = await post("/queues/tombstone-pressure/claims", {
      workerId: "worker",
    });

    const secondClaim = await secondClaimResponse.json<ClaimResponse>();
    const secondJob = secondClaim.jobs[0];

    if (!secondJob) throw new Error("claim returned no job");

    const secondLeaseToken = secondJob.lease?.token;

    if (!secondLeaseToken) throw new Error("claim returned a job without a lease token");

    const secondCompletion = await post(
      `/queues/tombstone-pressure/jobs/${secondJob.id}/complete`,
      { leaseToken: secondLeaseToken },
    );

    expect(secondCompletion.status).toBe(200);

    const pendingPush = await post("/queues/tombstone-pressure/jobs", {
      payload: "x".repeat(800),
    });

    expect(pendingPush.status).toBe(201);

    const beforePressure = await exports.default.fetch(
      "https://queue.test/queues/tombstone-pressure",
    );

    const previousState = await beforePressure.json<QueueState>();
    const oldestJobId = previousState.completed[0]?.jobId;

    expect(previousState.completed).toHaveLength(2);
    expect(oldestJobId).toBe(firstJob.id);

    const pressureClaim = await post("/queues/tombstone-pressure/claims", {
      workerId: "worker",
    });

    expect(pressureClaim.status).toBe(200);
    expect((await pressureClaim.json<ClaimResponse>()).jobs).toHaveLength(1);

    const afterPressure = await exports.default.fetch(
      "https://queue.test/queues/tombstone-pressure",
    );

    const state = await afterPressure.json<QueueState>();

    expect(state.completed.map((completed) => completed.jobId)).toEqual([secondJob.id]);
    expect(state.completed.map((completed) => completed.jobId)).not.toContain(oldestJobId);
  });
});

function post(path: string, body: JsonValue): Promise<Response> {
  return exports.default.fetch(`https://queue.test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

interface ClaimResponse {
  jobs: Array<{ id: string; lease?: { token: string } }>;
}

interface QueueState {
  jobs: unknown[];
  completed: Array<{ jobId: string }>;
}
