// src/routes/tradeAggRoutes.ts
import { Router } from "express";
import { pool } from "../db";
import { ENV } from "../config/env";

type TradeAggRow = {
  gameId: string;

  league: string;
  dateTs: number; // seconds (use lock_time)
  gameLabel: string;

  side: "A" | "B" | null;
  predictionCode: string;
  predictionColor: string;

  buyGross: number;
  allInPriceBps: number | null;

  returnAmount: number;
  claimAmount?: number;
  sellAmount?: number;

  teamACode?: string;
  teamBCode?: string;

  isFinal?: boolean;
  winnerSide?: "A" | "B" | "TIE" | null;

  roi: number | null;

  action: "Won" | "Lost" | "Sold" | "Tie" | "Pending";
  lastActivityTs: number;
};

function clampPageSize(v: any) {
  const n = parseInt(String(v || "10"), 10);
  const min = 1;
  const max = 50;
  return Math.max(min, Math.min(max, n));
}

function clampPage(v: any) {
  const n = parseInt(String(v || "1"), 10);
  return Math.max(1, n);
}

function normAddr(a: string) {
  return (a || "").toLowerCase();
}

function assertAddr(address: string) {
  return /^0x[a-f0-9]{40}$/.test(address);
}

function rangeToWindow(range: string) {
  const r = String(range || "ALL").toUpperCase();
  const nowSec = Math.floor(Date.now() / 1000);
  const farFuture = 4102444800;

  if (r === "D30") return { start: nowSec - 30 * 86400, end: nowSec };
  if (r === "D90") return { start: nowSec - 90 * 86400, end: nowSec };
  return { start: 0, end: farFuture };
}

function safeNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export const tradeAggRoutes = Router();

/**
 * GET /trade-agg/user/:address
 * Query:
 *  - league=ALL | NFL | ...
 *  - range=ALL | D30 | D90
 *  - page=1
 *  - pageSize=10
 *
 * Returns:
 *  { ok: true, rows: TradeAggRow[], page, pageSize, totalRows }
 */
tradeAggRoutes.get("/user/:address", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ ok: false, error: "Invalid address" });

  const league = String(req.query.league || "ALL").toUpperCase();
  const range = String(req.query.range || "ALL").toUpperCase();

  const page = clampPage(req.query.page);
  const pageSize = clampPageSize(req.query.pageSize);
  const offset = (page - 1) * pageSize;

  const { start, end } = rangeToWindow(range);

  // Filtering:
  const leagueFilterSql = league === "ALL" ? "" : `AND g.league = $3`;
  const leagueParam = league === "ALL" ? null : league;

  // We aggregate by (game_id, side) so that if a user could ever trade both sides,
  // you still get distinct rows. Your current product likely prevents this, but it's safe.
  const client = await pool.connect();
  try {
    // total rows count for paging
    const countSql = `
      SELECT COUNT(*)::int AS cnt
      FROM (
        SELECT e.game_id, e.side
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $4
          ${leagueFilterSql}
        GROUP BY e.game_id, e.side
      ) x
    `;

    const countParams =
      league === "ALL" ? [address, start, end] : [address, start, leagueParam, end];

    const countRes = await client.query(countSql, countParams);
    const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

    const sql = `
      WITH agg AS (
        SELECT
          e.game_id,
          e.side,

          -- BUY totals
          COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)::numeric AS buy_gross,

          -- Weighted price bps for BUYs (use avg_price_bps if present, else spot_price_bps)
          CASE
            WHEN COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0) > 0 THEN
              (
                SUM(
                  (COALESCE(e.avg_price_bps, e.spot_price_bps)::numeric)
                  * (e.gross_in_dec::numeric)
                ) FILTER (WHERE e.type = 'BUY')
                /
                SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY')
              )
            ELSE NULL
          END AS all_in_price_bps,

          -- SELL totals (full exit; treat as closed)
          COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type = 'SELL'), 0)::numeric AS sell_amount,

          -- last activity
          MAX(e.timestamp)::bigint AS last_activity_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $4
          ${leagueFilterSql}
        GROUP BY e.game_id, e.side
      )
      SELECT
        a.game_id,
        a.side,
        a.buy_gross,
        a.all_in_price_bps,
        a.sell_amount,
        a.last_activity_ts,

        g.league,
        g.lock_time,
        g.is_final,
        g.winner_side,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name
      FROM agg a
      JOIN public.games g ON g.game_id = a.game_id
      ORDER BY a.last_activity_ts DESC
      LIMIT $5 OFFSET $6
    `;

    const params =
      league === "ALL"
        ? [address, start, end, end, pageSize, offset] // placeholder, corrected below
        : [address, start, leagueParam, end, pageSize, offset];

    // Fix param positions (because leagueFilterSql uses $3 only when league != ALL)
    // When league === ALL, our placeholders are $1=user, $2=start, $4=end in the SQL above,
    // so we pass [$1,$2,$4,$5,$6] by duplicating end as third param to keep indexing aligned.
    // We'll rewrite cleanly:
    let finalParams: any[];
    if (league === "ALL") {
      // $1 address, $2 start, $4 end, $5 limit, $6 offset; we use $3 as a harmless filler
      finalParams = [address, start, end, end, pageSize, offset];
    } else {
      // $1 address, $2 start, $3 league, $4 end, $5 limit, $6 offset
      finalParams = [address, start, leagueParam, end, pageSize, offset];
    }

    const out = await client.query(sql, finalParams);

    const rows: TradeAggRow[] = (out.rows || []).map((r: any) => {
      const side: "A" | "B" | null = r.side === "B" ? "B" : r.side === "A" ? "A" : null;

      const teamACode = r.team_a_code ? String(r.team_a_code) : undefined;
      const teamBCode = r.team_b_code ? String(r.team_b_code) : undefined;
      const teamAName = r.team_a_name ? String(r.team_a_name) : "";
      const teamBName = r.team_b_name ? String(r.team_b_name) : "";

      const predictionCode =
        side === "A"
          ? (teamACode || "A")
          : side === "B"
            ? (teamBCode || "B")
            : "—";

      const isFinal = r.is_final == null ? undefined : Boolean(r.is_final);

      // Winner mapping:
      // - if is_final and winner_side is null => treat as TIE
      // - else A/B
      let winnerSide: "A" | "B" | "TIE" | null | undefined = null;
      const ws = r.winner_side == null ? null : String(r.winner_side);
      if (isFinal) {
        if (ws === "A" || ws === "B") winnerSide = ws as any;
        else winnerSide = "TIE";
      } else {
        winnerSide = null;
      }

      const buyGross = safeNum(r.buy_gross);
      const sellAmount = safeNum(r.sell_amount);
      const allInPriceBps =
        r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

      // Since your product is "sell full position", any SELL means closed.
      // Return is sell proceeds (for now). Claims can be added later.
      const returnAmount = sellAmount > 0 ? sellAmount : 0;

      // Action:
      let action: TradeAggRow["action"] = "Pending";
      if (sellAmount > 0) action = "Sold";
      else if (isFinal && winnerSide === "TIE") action = "Tie";
      else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
        action = winnerSide === side ? "Won" : "Lost";
      } else action = "Pending";

      // ROI:
      // - If Sold: realized ROI from sell vs buy
      // - Else: null (until claims are ingested)
      const roi =
        sellAmount > 0 && buyGross > 0 ? (sellAmount - buyGross) / buyGross : null;

      const gameLabel =
        teamAName && teamBName
          ? `${teamAName} vs ${teamBName}`
          : teamACode && teamBCode
            ? `${teamACode} vs ${teamBCode}`
            : String(r.game_id);

      return {
        gameId: String(r.game_id),
        league: String(r.league || "—"),
        dateTs: Number(r.lock_time || r.last_activity_ts || 0),
        gameLabel,

        side,
        predictionCode,
        predictionColor: "rgba(230,215,181,0.85)",

        buyGross,
        allInPriceBps,

        returnAmount,
        sellAmount: sellAmount > 0 ? sellAmount : undefined,

        teamACode,
        teamBCode,

        isFinal,
        winnerSide: winnerSide ?? null,

        roi,

        action,
        lastActivityTs: Number(r.last_activity_ts || 0),
      };
    });

    res.json({ ok: true, rows, page, pageSize, totalRows });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  } finally {
    client.release();
  }
});

export default tradeAggRoutes;
