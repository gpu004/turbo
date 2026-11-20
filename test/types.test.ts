import { Effect } from "effect";
import { describe, expectTypeOf, it } from "vitest";
import { Instant, JobId, LeaseToken, QueueName, WorkerId } from "../src/domain";
import {
  applyMutation,
  emptyQueue,
  openDraft,
  publicJob,
  type LeasedJob,
  type PublicJob,
  type QueueJob,
} from "../src/model";
import { route } from "../src/router";

// Each `@ts-expect-error` below is a mistake the types must reject. If one ever
// compiles, `tsc --noEmit` fails on the unused directive, so CI catches the
// regression even though nothing here runs.

const now = Instant.make("2026-08-31T12:00:00.000Z");

const jobId = JobId.make("job-1");

const token = LeaseToken.make("lease-1");

const draft = openDraft(emptyQueue(QueueName.make("q"), now), () => token);

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
      workerId: WorkerId.make("w"),
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

  it("keeps a lease token out of a job shown to anyone but its holder", () => {
    const leased: LeasedJob = {
      id: jobId,
      payload: null,
      enqueuedAt: now,
      attempts: 1,
      state: "leased",
      lease: { token, workerId: WorkerId.make("w"), heartbeatAt: now, expiresAt: now },
    };

    // @ts-expect-error a leased job still carries its token
    const leaked: PublicJob = leased;

    const shown: PublicJob = publicJob(leased);

    expectTypeOf(leaked).toEqualTypeOf(shown);
  });

  it("only exposes route params the pattern captures", () => {
    route("GET", "/queues/:queue", (_context: null, params) => {
      expectTypeOf(params.queue).toEqualTypeOf<string>();

      // @ts-expect-error the pattern has no :jobId
      void params.jobId;

      return Effect.succeed(new Response());
    });
  });
});
