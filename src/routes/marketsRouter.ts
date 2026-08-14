// src/routes/marketsRouter.ts
//
// GET /api/markets/snapshot — server-cached AMM pool state for the homepage
// market cards. One Multicall3 per ≤30s server-side, shared across ALL users,
// so the browser makes ZERO eth_calls to render cards (previously each card
// fired its own 8-call multicall every ~30s, per user — the bulk of the
// Alchemy eth_call bill).

import { Router, Request, Response } from "express";
import { getMarketSnapshots } from "../services/marketSnapshot";

const router = Router();

router.get("/snapshot", async (_req: Request, res: Response) => {
  try {
    const out = await getMarketSnapshots();
    // Let the CDN/browser hold it briefly too — the data only changes every ~30s.
    res.set("Cache-Control", "public, max-age=15");
    return res.json(out);
  } catch (e: any) {
    console.error("[marketsRouter/snapshot]", e?.message || e);
    return res.status(500).json({ error: "snapshot_failed" });
  }
});

export default router;
