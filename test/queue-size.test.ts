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
