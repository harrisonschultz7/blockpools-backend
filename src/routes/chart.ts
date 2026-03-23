// src/routes/chart.ts
//
// GET /api/chart/:contractAddress
//
// Returns BUY trade events for a contract from user_trade_events,
// ordered by timestamp asc — used by LeagueWinnerChart to build
// the price-over-time series without touching the subgraph.
//
// Response: Array<{ outcome_index: number; spot_price_bps: number; timestamp: number }>
//
// Deploy: add to your Express router in server.ts / index.ts:
//   import chartRouter from "./routes/chart";
//   app.use("/api/chart", chartRouter);

import { Router, Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";

const router = Router();

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

// In-memory cache — avoids hammering Supabase on every chart mount
const _cache = new Map<string, { data: any[]; ts: number }>();
const CACHE_TTL_MS = 60_000; // 60s — stale data is fine for chart rendering

router.get("/:contractAddress", async (req: Request, res: Response) => {
  const { contractAddress } = req.params;

  if (!contractAddress || !/^0x[0-9a-fA-F]{40}$/.test(contractAddress)) {
    return res.status(400).json({ error: "Invalid contract address" });
  }

  const gameId = contractAddress.toLowerCase();

  // Serve from cache if fresh
  const cached = _cache.get(gameId);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return res.json(cached.data);
  }

  try {
    const { data, error } = await supabase
      .from("user_trade_events")
      .select("outcome_index, spot_price_bps, timestamp")
      .eq("game_id", gameId)
      .eq("type", "BUY")
      .not("outcome_index", "is", null)
      .not("spot_price_bps", "is", null)
      .order("timestamp", { ascending: true })
      .limit(2000); // ample for any futures market

    if (error) {
      console.error("[chart] Supabase error:", error.message);
      return res.status(500).json({ error: "Database error" });
    }

    const result = (data ?? []).map((r: any) => ({
      outcome_index: Number(r.outcome_index),
      spot_price_bps: Number(r.spot_price_bps),
      timestamp: Number(r.timestamp),
    }));

    _cache.set(gameId, { data: result, ts: Date.now() });
    return res.json(result);
  } catch (e: any) {
    console.error("[chart] Unexpected error:", e?.message);
    return res.status(500).json({ error: "Internal error" });
  }
});

export default router;