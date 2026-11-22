import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: { QUEUE_BATCH_WINDOW_MS: "100", QUEUE_COMMIT_INTERVAL_MS: "0" },
      },
    }),
  ],
});
