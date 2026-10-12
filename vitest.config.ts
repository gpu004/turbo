import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/commit.test.ts", "test/model.test.ts", "test/worker.test.ts"],
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: { bindings: { QUEUE_COMMIT_INTERVAL_MS: "0" } },
    }),
  ],
});
