import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        // The Worker tests assert that concurrent requests share one commit.
        // The production 5 ms window is too short for a loaded CI runner to
        // deliver both requests inside it, so tests use a wider one.
        // They also make many mutations in a row, which the production pace of
        // one R2 write per second would slow to several seconds each test.
        bindings: { QUEUE_BATCH_WINDOW_MS: "100", QUEUE_COMMIT_INTERVAL_MS: "0" },
      },
    }),
  ],
});
