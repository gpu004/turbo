import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/overload.test.ts"],
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          MAX_PENDING_MUTATIONS: "1",
          QUEUE_BATCH_WINDOW_MS: "50",
          QUEUE_COMMIT_INTERVAL_MS: "0",
        },
      },
    }),
  ],
});
