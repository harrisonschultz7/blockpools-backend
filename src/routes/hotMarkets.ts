// src/routes/hotMarkets.ts
//
// GET /api/hot-markets/one-sided
//
// Returns markets whose BUY volume landed on EXACTLY ONE outcome (the other
// side has received $0 of buys) — the house is unbalanced/exposed on these, so
// the home page promotes them to encourage users to buy the other side.
//
// game_id in user_trade_events is the AMM contract address (0x…40 hex) for v1
// pools and the v2 gameId string (e.g. "MLB-BOS-LAD-2026-07-31-…") for
// order-book markets. Both are returned as-is; the frontend intersects them
// with its open-game catalogue (by contractAddress for AMM, gameId for v2).
//
// Response: Array<{ game_id: string; side_code: string; buy_vol: number; buy_events: number }>
//           ordered by buy_vol desc (biggest one-sided exposure first).
//
// Deploy: register in server.ts:
//   import hotMarketsRouter from "./routes/hotMarkets";
//   app.use("/api/hot-markets", hotMarketsRouter);

import { Router, Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";

const router = Router();

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const WINDOW_SECONDS = 45 * 24 * 60 * 60; // 45d — bounds the scan; open games are recent
const CACHE_TTL_MS = 60_000;
let _cache: { data: any[]; ts: number } | null = null;

router.get("/one-sided", async (req: Request, res: Response) => {
  const bypass = req.query.refresh === "1" || req.query.nocache === "1";
  if (!bypass && _cache && Date.now() - _cache.ts < CACHE_TTL_MS) {
    return res.json(_cache.data);
  }

  try {
    const sinceTs = Math.floor(Date.now() / 1000) - WINDOW_SECONDS;
    const { data, error } = await supabase
      .from("user_trade_events")
      .select("game_id, outcome_index, outcome_code, gross_in_dec")
      .eq("type", "BUY")
      .not("outcome_index", "is", null)
      .gte("timestamp", sinceTs)
      .limit(20000);

    if (error) {
      console.error("[hot-markets] Supabase error:", error.message);
      return res.json([]);
    }

    // Aggregate per market: distinct outcomes bought + total buy volume.
    const byGame = new Map<
      string,
      { outcomes: Set<number>; vol: number; events: number; code: string }
    >();
    for (const r of data ?? []) {
      const key = String(r.game_id || "").toLowerCase();
      if (!key) continue;
      const outcome = Number(r.outcome_index);
      if (!Number.isFinite(outcome)) continue;
      let agg = byGame.get(key);
      if (!agg) {
        agg = { outcomes: new Set(), vol: 0, events: 0, code: "" };
        byGame.set(key, agg);
      }
      agg.outcomes.add(outcome);
      agg.vol += Number(r.gross_in_dec) || 0;
      agg.events += 1;
      if (r.outcome_code) agg.code = String(r.outcome_code);
    }

    // Strictly one-sided: buys hit exactly one outcome (other side has $0).
    const result = [...byGame.entries()]
      .filter(([, v]) => v.outcomes.size === 1)
      .map(([game_id, v]) => ({
        game_id,
        side_code: v.code,
        buy_vol: Math.round(v.vol * 100) / 100,
        buy_events: v.events,
      }))
      .sort((a, b) => b.buy_vol - a.buy_vol)
      .slice(0, 100);

    _cache = { data: result, ts: Date.now() };
    return res.json(result);
  } catch (e: any) {
    console.error("[hot-markets] Unexpected error:", e?.message);
    return res.json([]);
  }
});

export default router;
