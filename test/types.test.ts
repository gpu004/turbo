import { describe, expectTypeOf, it } from "vitest";
import { Instant, JobId, LeaseToken, QueueName, WorkerId } from "../src/domain";
import { applyMutation, emptyQueue, openDraft, type LeasedJob, type QueueJob } from "../src/model";
import { route } from "../src/router";

// Each `@ts-expect-error` below is a mistake the types must reject. If one ever
// compiles, `tsc --noEmit` fails on the unused directive, so CI catches the
// regression even though nothing here runs.

const now = Instant.parse("2026-08-31T12:00:00.000Z");

const jobId = JobId.parse("job-1");

const token = LeaseToken.parse("lease-1");

const draft = openDraft(emptyQueue(QueueName.parse("q"), now), () => token);

describe("compile-time guarantees", () => {
  it("rejects unvalidated or swapped identifiers", () => {
    // @ts-expect-error a raw string is not a JobId
    applyMutation(draft, { kind: "complete", jobId: "job-1", leaseToken: token, now });

    // @ts-expect-error jobId and leaseToken are swapped
    applyMutation(draft, { kind: "complete", jobId: token, leaseToken: jobId, now });

    // @ts-expect-error a raw string is not an Instant
    applyMutation(draft, { kind: "complete", jobId, leaseToken: token, now: "yesterday" });
  });

  it("rejects mutations missing fields for their kind", () => {
    // @ts-expect-error heartbeat needs leaseSeconds
    applyMutation(draft, { kind: "heartbeat", jobId, leaseToken: token, now });
  });

  it("ties each mutation kind to its own result type", () => {
    const result = applyMutation(draft, {
      kind: "claim",
      workerId: WorkerId.parse("w"),
      maxJobs: 1,
      leaseSeconds: 60,
      now,
    });

    expectTypeOf(result.kind).toEqualTypeOf<"claim">();
    expectTypeOf(result.jobs).toEqualTypeOf<readonly LeasedJob[]>();
  });

  it("makes a lease-less leased job and job mutation unrepresentable", () => {
    const fields = { id: jobId, payload: null, enqueuedAt: now, attempts: 0 };

    // @ts-expect-error a leased job must carry a lease
    const leaseless: QueueJob = { ...fields, state: "leased" };

    const pending: QueueJob = { ...fields, state: "pending" };

    // @ts-expect-error jobs are immutable values
    pending.attempts = 1;

    expectTypeOf(leaseless).toEqualTypeOf<QueueJob>();
  });

  it("only exposes route params the pattern captures", () => {
    route("GET", "/queues/:queue", async (_context: null, params) => {
      expectTypeOf(params.queue).toEqualTypeOf<string>();

      // @ts-expect-error the pattern has no :jobId
      void params.jobId;

      return new Response();
    });
  });
});
