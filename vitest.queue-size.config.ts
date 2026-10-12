import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/queue-size.test.ts"],
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: { MAX_QUEUE_BYTES: "1500", QUEUE_COMMIT_INTERVAL_MS: "0" },
      },
    }),
  ],
});
