import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { JsonValue } from "../src/model";

describe("broker overload", () => {
  it("returns retryable overload responses while accepting available work", async () => {
    const responses = await Promise.all(
      Array.from({ length: 5 }, (_, index) =>
        post("/queues/overloaded/jobs", { payload: { index } }),
      ),
    );

    const accepted = responses.find((response) => response.status === 201);
    const overloaded = responses.find((response) => response.status === 503);

    expect(accepted?.status).toBe(201);
    expect(overloaded?.headers.get("retry-after")).toBe("1");
    expect(await overloaded?.json()).toMatchObject({
      error: { code: "overloaded" },
    });
  });
});

function post(path: string, body: JsonValue): Promise<Response> {
  return exports.default.fetch(`https://queue.test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}
