// src/routes/v2Routes.ts
//
// Lifecycle + read API for the v2 (order-book) exchange. The matcher POSTs
// order/fill/cancel events here as they happen on-chain; the frontend reads
// open orders (durable Open Orders tab) and the monitoring stats.
//
// Write endpoints are guarded by an optional shared secret (V2_MATCHER_SECRET):
// enforced only when the env is set, so localdev works without it while prod can
// lock the matcher->backend channel.

import { Router } from "express";
import { pool } from "../db";
import {
  recordV2Order,
  recordV2Fill,
  cancelV2Order,
  getOpenV2Orders,
  recordV2Resolution,
} from "../services/v2/v2Persist";
import { invalidateV2Markets, v2MarketsSource } from "../services/v2/v2MarketResolver";

const v2Router = Router();

const V2_MATCHER_SECRET = (process.env.V2_MATCHER_SECRET || "").trim();

function requireSecret(req: any, res: any): boolean {
  if (!V2_MATCHER_SECRET) return true; // open in localdev
  const got = String(req.headers["x-v2-secret"] || "");
  if (got && got === V2_MATCHER_SECRET) return true;
  res.status(401).json({ ok: false, error: "unauthorized" });
  return false;
}

// --- Matcher lifecycle events (writes) --------------------------------------

// POST /api/v2/order  — upsert an order (accept / rest / counter-fill update).
// Body: { hash, maker, marketId, outcome, side, shares, price, feeRateBps?,
//         expiry?, salt, signature, remaining?, filledShares?, status? }
v2Router.post("/order", async (req, res) => {
  if (!requireSecret(req, res)) return;
  try {
    const b = req.body || {};
    if (!b.hash || !b.maker || !b.marketId || b.signature == null) {
      return res.status(400).json({ ok: false, error: "hash, maker, marketId, signature required" });
    }
    await recordV2Order(b);
    return res.json({ ok: true });
  } catch (e: any) {
    console.error("[v2/order]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// POST /api/v2/fill — record one settled fill (both legs) + map to stats.
// Body: { txHash, matchType, fill, makerPrice, a:{...leg}, b:{...leg}, blockNumber? }
v2Router.post("/fill", async (req, res) => {
  if (!requireSecret(req, res)) return;
  try {
    const b = req.body || {};
    if (!b.txHash || !b.a || !b.b || !b.matchType) {
      return res.status(400).json({ ok: false, error: "txHash, matchType, a, b required" });
    }
    const out = await recordV2Fill(b);
    return res.json({ ok: true, ...out });
  } catch (e: any) {
    console.error("[v2/fill]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// POST /api/v2/cancel — mark an order cancelled. Body: { hash }
v2Router.post("/cancel", async (req, res) => {
  if (!requireSecret(req, res)) return;
  try {
    const hash = req.body?.hash;
    if (!hash) return res.status(400).json({ ok: false, error: "hash required" });
    await cancelV2Order(String(hash));
    return res.json({ ok: true });
  } catch (e: any) {
    console.error("[v2/cancel]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// POST /api/v2/resolve — a market resolved on-chain; write realized-PnL CLAIM
// rows for winners + clear resting orders. Body: { marketId, winningOutcome, gameId? }
// winningOutcome: 0 | 1 | null (null = even void [1,1]).
v2Router.post("/resolve", async (req, res) => {
  if (!requireSecret(req, res)) return;
  try {
    const b = req.body || {};
    if (!b.marketId) return res.status(400).json({ ok: false, error: "marketId required" });
    const win =
      b.winningOutcome == null || b.winningOutcome === "" ? null : Number(b.winningOutcome);
    const out = await recordV2Resolution({
      marketId: String(b.marketId),
      winningOutcome: win,
      gameId: b.gameId ? String(b.gameId) : undefined,
    });
    return res.json({ ok: true, ...out });
  } catch (e: any) {
    console.error("[v2/resolve]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// POST /api/v2/reload-markets — force a games.json re-read (call after a deploy).
v2Router.post("/reload-markets", async (req, res) => {
  if (!requireSecret(req, res)) return;
  invalidateV2Markets();
  return res.json({ ok: true, source: v2MarketsSource() });
});

// --- Reads ------------------------------------------------------------------

// GET /api/v2/redeemable?maker=<addr> — resolved markets the user traded. The
// frontend reads on-chain (markets/payoutNum/sharesOf) to decide which are
// winning + unredeemed and shows a Redeem button; losers/redeemed drop off.
v2Router.get("/redeemable", async (req, res) => {
  const maker = String(req.query.maker || "").trim().toLowerCase();
  if (!maker) return res.status(400).json({ ok: false, error: "maker required" });
  // all=1 skips the is_final filter → every market the wallet traded (the caller
  // decides resolved/redeemable on-chain). Used by the operator claim-all script.
  const all = String(req.query.all || "") === "1";
  try {
    const { rows: markets } = await pool.query(
      `SELECT f.market_id AS "marketId", max(f.game_id) AS "gameId", max(f.league) AS league
         FROM v2.fills f
         LEFT JOIN public.games g ON g.game_id = f.game_id
        WHERE (lower(f.a_maker) = $1 OR lower(f.b_maker) = $1)
          ${all ? "" : "AND COALESCE(g.is_final, false) = true"}
        GROUP BY f.market_id`,
      [maker]
    );
    const gameIds = markets.map((m: any) => m.gameId).filter(Boolean);

    // Ledger cost + net shares per (game, outcome) so the card can show Amount
    // Traded, avg Price and ROI against the on-chain payout.
    const byKey = new Map<string, { cost: number; shares: number }>();
    if (gameIds.length) {
      const { rows: agg } = await pool.query(
        `SELECT game_id, outcome_index,
                SUM(CASE WHEN type='BUY' THEN COALESCE(gross_in_dec,0) ELSE 0 END)::float8 AS cost,
                SUM(CASE WHEN type='BUY'  THEN COALESCE(gross_in_dec,0)/(avg_price_bps/10000.0)
                         WHEN type='SELL' THEN -COALESCE(gross_out_dec,net_out_dec,0)/(avg_price_bps/10000.0)
                         ELSE 0 END)::float8 AS shares
           FROM public.user_trade_events
          WHERE lower(user_address) = $1 AND type IN ('BUY','SELL') AND avg_price_bps > 0
            AND game_id = ANY($2)
          GROUP BY game_id, outcome_index`,
        [maker, gameIds]
      );
      for (const a of agg as any[]) {
        byKey.set(`${a.game_id}:${a.outcome_index}`, { cost: Number(a.cost) || 0, shares: Number(a.shares) || 0 });
      }
    }

    const out = markets.map((m: any) => ({
      marketId: m.marketId,
      gameId: m.gameId,
      league: m.league,
      outcomes: {
        0: byKey.get(`${m.gameId}:0`) || { cost: 0, shares: 0 },
        1: byKey.get(`${m.gameId}:1`) || { cost: 0, shares: 0 },
      },
    }));
    res.setHeader("Cache-Control", "no-store");
    return res.json({ ok: true, markets: out });
  } catch (e: any) {
    console.error("[v2/redeemable]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// GET /api/v2/open-orders?marketId=&maker=&limit=  — rehydrate + Open Orders tab.
v2Router.get("/open-orders", async (req, res) => {
  try {
    const rows = await getOpenV2Orders({
      marketId: req.query.marketId ? String(req.query.marketId) : undefined,
      maker: req.query.maker ? String(req.query.maker) : undefined,
      limit: req.query.limit ? Number(req.query.limit) : undefined,
    });
    res.setHeader("Cache-Control", "no-store");
    return res.json({ ok: true, orders: rows });
  } catch (e: any) {
    console.error("[v2/open-orders]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// GET /api/v2/chart/:gameId — per-outcome traded-price history for a v2 market.
//
// Unlike the AMM chart (which stores a PRE-trade spot price and must simulate the
// book forward), v2 fills store the ACTUAL per-outcome price in bps at each trade,
// and a binary market's two legs are complementary (sum ~10000 bps). So the series
// needs no reconstruction — just the real points, oldest first. Matches the
// market's own game_id case-insensitively, plus any `PARENT::CODE` sub-market
// game_ids (group / league-winner markets). Every leg is a v2- prefixed row.
//
// Response: { ok, points: [{ ts, outcomeIndex, code, bps }] }
v2Router.get("/chart/:gameId", async (req, res) => {
  const gameId = String(req.params.gameId || "").trim();
  if (!gameId) return res.status(400).json({ ok: false, error: "gameId required" });
  try {
    const { rows } = await pool.query(
      `SELECT timestamp AS ts, outcome_index, outcome_code, spot_price_bps
         FROM public.user_trade_events
        WHERE id LIKE 'v2-%'
          AND type IN ('BUY','SELL')
          AND spot_price_bps IS NOT NULL
          AND (game_id ILIKE $1 OR game_id ILIKE $1 || '::%')
        ORDER BY timestamp ASC
        LIMIT 3000`,
      [gameId]
    );
    const points = rows
      .map((r: any) => ({
        ts: Number(r.ts),
        outcomeIndex: r.outcome_index == null ? null : Number(r.outcome_index),
        code: r.outcome_code ?? null,
        bps: Number(r.spot_price_bps),
      }))
      .filter((p) => Number.isFinite(p.ts) && p.ts > 0 && Number.isFinite(p.bps));
    res.setHeader("Cache-Control", "public, max-age=10, stale-while-revalidate=30");
    return res.json({ ok: true, points });
  } catch (e: any) {
    console.error("[v2/chart]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// GET /api/v2/stats — basic monitoring: volume + trade count (all-time + 24h),
// plus a per-market breakdown. Volume = notional USD across both legs of each
// fill = maker_price * fill_shares / 1e12 (one complete-set side of the trade).
v2Router.get("/stats", async (_req, res) => {
  try {
    const [totals, byMarket, recent] = await Promise.all([
      pool.query(`
        SELECT
          count(*)::int AS fills,
          count(*) FILTER (WHERE created_at > now() - interval '24 hours')::int AS fills_24h,
          COALESCE(SUM(maker_price * fill_shares) / 1e12, 0)::numeric(20,2) AS notional_usd,
          COALESCE(SUM(maker_price * fill_shares) FILTER (WHERE created_at > now() - interval '24 hours') / 1e12, 0)::numeric(20,2) AS notional_usd_24h,
          count(DISTINCT market_id)::int AS markets
        FROM v2.fills
      `),
      pool.query(`
        SELECT market_id, game_id, league,
               count(*)::int AS fills,
               COALESCE(SUM(maker_price * fill_shares) / 1e12, 0)::numeric(20,2) AS notional_usd
        FROM v2.fills
        GROUP BY market_id, game_id, league
        ORDER BY notional_usd DESC
        LIMIT 100
      `),
      pool.query(`
        SELECT id AS tx_hash, market_id, game_id, match_type, fill_shares, maker_price,
               a_maker, b_maker, created_at
        FROM v2.fills
        ORDER BY created_at DESC
        LIMIT 25
      `),
    ]);
    res.setHeader("Cache-Control", "public, max-age=5, stale-while-revalidate=30");
    return res.json({
      ok: true,
      totals: totals.rows[0],
      byMarket: byMarket.rows,
      recent: recent.rows,
    });
  } catch (e: any) {
    console.error("[v2/stats]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

export default v2Router;
