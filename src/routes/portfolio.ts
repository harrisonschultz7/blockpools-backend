// src/routes/portfolio.ts
//
// Portfolio net-worth time-series for the profile chart.
//
// The equity number itself is computed CLIENT-side (the profile already reads
// on-chain USDC + open positions marked to the live book price), so the server
// only needs to persist a throttled history to draw the curve — no server-side
// pricing pipeline. Forward-only: the chart fills in from the first snapshot.
//
//   POST /api/portfolio/:address/snapshot   (auth; owner-only; <=1/hour)
//        body { cashUsd, positionsUsd, equityUsd }
//   GET  /api/portfolio/:address/series?range=ALL|D30|D90   (public)
//        -> { ok, latest, points: [{ t, equity, cash, positions }] }

import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

const SNAPSHOT_MIN_GAP_MIN = Number(process.env.PORTFOLIO_SNAPSHOT_GAP_MIN || 60);

const normAddr = (a: any) => String(a || "").trim().toLowerCase();
const num = (v: any) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

// POST /api/portfolio/:address/snapshot — record one net-worth point.
// Owner-only (you can only snapshot your own wallet) + throttled so a user
// refreshing their profile doesn't spam the series.
router.post("/:address/snapshot", authPrivy, async (req: AuthedRequest, res: Response) => {
  const address = normAddr(req.params.address);
  const owner = normAddr(req.user?.primaryAddress);
  if (!address) return res.status(400).json({ ok: false, error: "address required" });
  if (!owner || owner !== address) {
    return res.status(403).json({ ok: false, error: "can only snapshot your own portfolio" });
  }

  const cash = num(req.body?.cashUsd);
  const positions = num(req.body?.positionsUsd);
  const equity = req.body?.equityUsd != null ? num(req.body.equityUsd) : cash + positions;

  try {
    // Throttle: skip if we already have a snapshot within the min-gap window.
    const recent = await pool.query(
      `SELECT 1 FROM public.portfolio_snapshots
        WHERE user_address = $1 AND snapshot_at > now() - ($2 || ' minutes')::interval
        LIMIT 1`,
      [address, SNAPSHOT_MIN_GAP_MIN]
    );
    if (recent.rowCount) return res.json({ ok: true, skipped: "throttled" });

    await pool.query(
      `INSERT INTO public.portfolio_snapshots (user_address, cash_usd, positions_usd, equity_usd)
       VALUES ($1, $2, $3, $4)`,
      [address, cash, positions, equity]
    );
    return res.json({ ok: true });
  } catch (e: any) {
    console.error("[portfolio/snapshot]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// GET /api/portfolio/:address/series — the chart series + latest point. Public
// (on-chain balances are public), so a visitor sees the owner's last-known value.
router.get("/:address/series", async (req: AuthedRequest, res: Response) => {
  const address = normAddr(req.params.address);
  if (!address) return res.status(400).json({ ok: false, error: "address required" });

  const range = String(req.query.range || "ALL").toUpperCase();
  const days = range === "D30" ? 30 : range === "D90" ? 90 : range === "D7" ? 7 : null;

  try {
    const where = days
      ? `user_address = $1 AND pnl_usd IS NOT NULL AND snapshot_at > now() - ($2 || ' days')::interval`
      : `user_address = $1 AND pnl_usd IS NOT NULL`;
    const params: any[] = days ? [address, days] : [address];

    const { rows } = await pool.query(
      `SELECT extract(epoch from snapshot_at)::bigint AS t,
              pnl_usd::float8 AS pnl,
              positions_usd::float8 AS positions
         FROM public.portfolio_snapshots
        WHERE ${where}
        ORDER BY snapshot_at ASC
        LIMIT 2000`,
      params
    );
    const latest = rows.length ? rows[rows.length - 1] : null;
    res.setHeader("Cache-Control", "public, max-age=15, stale-while-revalidate=60");
    return res.json({ ok: true, latest, points: rows });
  } catch (e: any) {
    console.error("[portfolio/series]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// GET /api/portfolio/:address/positions — net OPEN positions derived from the
// unified trade ledger (works for v1 AMM + v2 order-book). Returns cost basis +
// avg entry per (game, outcome); the client marks to market with live prices.
// "Open" = net shares > 0 in a game that hasn't resolved yet.
router.get("/:address/positions", async (req: AuthedRequest, res: Response) => {
  const address = normAddr(req.params.address);
  if (!address) return res.status(400).json({ ok: false, error: "address required" });

  try {
    const { rows } = await pool.query(
      `SELECT t.game_id, t.type, t.outcome_index, t.outcome_code, t.league,
              t.avg_price_bps, t.gross_in_dec, t.gross_out_dec, t.net_out_dec,
              g.market_type, g.team_a_name, g.team_b_name, g.market_question,
              g.is_final, g.resolution_type, g.team_a_code, g.team_b_code
         FROM public.user_trade_events t
         LEFT JOIN public.games g ON g.game_id = t.game_id
        WHERE lower(t.user_address) = $1
          AND t.type IN ('BUY','SELL')
          AND t.outcome_index IS NOT NULL
          AND COALESCE(g.is_final, false) = false
          AND COALESCE(upper(g.resolution_type), 'UNRESOLVED') NOT IN ('RESOLVED','FINAL')`,
      [address]
    );

    // Derive net shares + weighted-avg entry per (game, outcome). shares = $ / price.
    type Agg = {
      gameId: string; league: string | null; outcomeIndex: number; outcomeCode: string | null;
      marketType: string | null; teamAName: string | null; teamBName: string | null;
      teamACode: string | null; teamBCode: string | null; marketQuestion: string | null;
      shares: number; cost: number; // net shares held, remaining cost basis
    };
    const agg = new Map<string, Agg>();
    for (const r of rows) {
      const priceBps = num(r.avg_price_bps);
      const price = priceBps > 0 ? priceBps / 10000 : 0; // $/share
      if (price <= 0) continue;
      const key = `${r.game_id}:${r.outcome_index}`;
      let a = agg.get(key);
      if (!a) {
        a = {
          gameId: r.game_id, league: r.league, outcomeIndex: Number(r.outcome_index),
          outcomeCode: r.outcome_code, marketType: r.market_type,
          teamAName: r.team_a_name, teamBName: r.team_b_name,
          teamACode: r.team_a_code, teamBCode: r.team_b_code, marketQuestion: r.market_question,
          shares: 0, cost: 0,
        };
        agg.set(key, a);
      }
      if (r.type === "BUY") {
        const sh = num(r.gross_in_dec) / price;
        a.shares += sh;
        a.cost += num(r.gross_in_dec);
      } else {
        // SELL: reduce position + release proportional cost basis
        const sellDollars = num(r.gross_out_dec) || num(r.net_out_dec);
        const sh = sellDollars / price;
        if (a.shares > 0) {
          const closeSh = Math.min(sh, a.shares);
          a.cost -= (a.cost * closeSh) / a.shares;
          a.shares -= closeSh;
        }
      }
    }

    const positions = [...agg.values()]
      .filter((a) => a.shares > 1e-6)
      .map((a) => ({
        gameId: a.gameId,
        league: a.league,
        marketType: a.marketType,
        outcomeIndex: a.outcomeIndex,
        outcomeCode: a.outcomeCode,
        teamAName: a.teamAName,
        teamBName: a.teamBName,
        teamACode: a.teamACode,
        teamBCode: a.teamBCode,
        marketQuestion: a.marketQuestion,
        shares: Number(a.shares.toFixed(6)),
        costBasisDec: Number(a.cost.toFixed(6)),
        avgPriceBps: a.shares > 0 ? Math.round((a.cost / a.shares) * 10000) : null,
      }))
      .sort((x, y) => y.costBasisDec - x.costBasisDec);

    res.setHeader("Cache-Control", "public, max-age=10, stale-while-revalidate=30");
    return res.json({ ok: true, positions });
  } catch (e: any) {
    console.error("[portfolio/positions]", e?.message || e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

export default router;
