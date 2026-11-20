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
        bindings: { QUEUE_BATCH_WINDOW_MS: "100" },
      },
    }),
  ],
});
