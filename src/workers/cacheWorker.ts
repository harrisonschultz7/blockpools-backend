// src/workers/cacheWorker.ts
import { ENV } from "../config/env";
import { keyLeaderboard } from "../cache/cacheKeys";
import { refreshLeaderboard } from "../services/cacheRefresh";

// A simple warmup loop; safe even if subgraph fails (errors are caught in refresh wrapper elsewhere)
// Here, we just log errors.
export function startCacheWorker() {
  if (!ENV.CACHE_WORKER_ENABLED) return;

  const DEFAULT_LEAGUES = ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

  const tick = async () => {
    const params = {
      leagues: DEFAULT_LEAGUES,
      range: "ALL",
      sort: "ROI",
      page: 1,
      pageSize: 25,
    };

    const cacheKey = keyLeaderboard(params);

    try {
      // Direct refresh (worker doesn’t need SWR wrapper)
      await refreshLeaderboard(params);
      // We don't write into cache directly here because refreshLeaderboard is pure.
      // If you want worker to populate SWR cache, call the route once instead.
      // Keeping worker minimal avoids accidental schema assumptions.
      console.log(`[cacheWorker] tick ok (${cacheKey}) @ ${new Date().toISOString()}`);
    } catch (e: any) {
      console.log(`[cacheWorker] tick err: ${String(e?.message || e)}`);
    }
  };

  // Run immediately and then on interval
  void tick();
  setInterval(() => void tick(), ENV.CACHE_WORKER_INTERVAL_SECONDS * 1000);
}
