import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    environment: "node",
    // Each suite owns a private temp DSH home; running them in one worker keeps
    // the fake-clock and filesystem fixtures from interleaving.
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
