// src/routes/tradeAggRoutes.ts
import { Router } from "express";
import { pool } from "../db";

type TradeAggRow = {
  gameId: string;

  league: string;
  dateTs: number;
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
  return (a || "").toLowerCase().trim();
}

function assertAddr(address: string) {
  return /^0x[a-f0-9]{40}$/.test(address);
}

function rangeToWindow(range: string) {
  const r = String(range || "ALL").toUpperCase().trim();
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

async function handleTradeAgg(req: any, res: any) {
  const address = normAddr(String(req.query.user || req.params.address || ""));
  if (!assertAddr(address)) {
    return res.status(400).json({ ok: false, error: "Invalid address" });
  }

  const league = String(req.query.league || "ALL").toUpperCase().trim();
  const range = String(req.query.range || "ALL").toUpperCase().trim();

  const page = clampPage(req.query.page);
  const pageSize = clampPageSize(req.query.pageSize);
  const offset = (page - 1) * pageSize;

  const { start, end } = rangeToWindow(range);

  // params: $1 addr, $2 start, $3 end, $4 league, $5 limit, $6 offset
  const params = [address, start, end, league, pageSize, offset];

  const client = await pool.connect();
  try {
    // Count distinct (game_id, side) rows within window.
    // NOTE: if you store CLAIM rows with side='C', they will be counted as their own group.
    // If you don't want that, change GROUP BY to (game_id, CASE WHEN side='C' THEN NULL ELSE side END)
    // and adjust the main query similarly. For now we keep it exact.
    const countSql = `
      SELECT COUNT(*)::int AS cnt
      FROM (
        SELECT e.game_id, e.side
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
        GROUP BY e.game_id, e.side
      ) x
    `;

    const countRes = await client.query(countSql, params.slice(0, 4));
    const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

    const sql = `
      WITH agg AS (
        SELECT
          e.game_id,
          e.side,

          -- BUY totals
          COALESCE(
            SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'),
            0
          )::numeric AS buy_gross,

          -- Weighted price bps for BUYs (all-in avg)
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

          -- SELL totals
          COALESCE(
            SUM(e.net_out_dec::numeric) FILTER (WHERE e.type = 'SELL'),
            0
          )::numeric AS sell_amount,

          -- CLAIM totals (payouts)
          COALESCE(
            SUM(e.net_out_dec::numeric) FILTER (WHERE e.type = 'CLAIM'),
            0
          )::numeric AS claim_amount,

          MAX(e.timestamp)::bigint AS last_activity_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
        GROUP BY e.game_id, e.side
      )
      SELECT
        a.game_id,
        a.side,
        a.buy_gross,
        a.all_in_price_bps,
        a.sell_amount,
        a.claim_amount,
        a.last_activity_ts,

        g.league,
        g.lock_time,
        g.is_final,
        g.winner_side,
        g.winner_team_code,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name
      FROM agg a
      JOIN public.games g ON g.game_id = a.game_id
      ORDER BY a.last_activity_ts DESC
      LIMIT $5 OFFSET $6
    `;

    const out = await client.query(sql, params);

    const rows: TradeAggRow[] = (out.rows || []).map((r: any) => {
      const side: "A" | "B" | null =
        r.side === "B" ? "B" : r.side === "A" ? "A" : null;

      const teamACode = r.team_a_code ? String(r.team_a_code) : undefined;
      const teamBCode = r.team_b_code ? String(r.team_b_code) : undefined;
      const teamAName = r.team_a_name ? String(r.team_a_name) : "";
      const teamBName = r.team_b_name ? String(r.team_b_name) : "";

      const predictionCode =
        side === "A" ? teamACode || "A" : side === "B" ? teamBCode || "B" : "—";

      const isFinal = r.is_final == null ? undefined : Boolean(r.is_final);

      // Winner mapping:
      // - If final and winner_team_code is TIE/DRAW OR winner_side is C => TIE
      // - Else A/B when present; otherwise treat missing as TIE for final games
      let winnerSide: "A" | "B" | "TIE" | null = null;
      const ws = r.winner_side == null ? "" : String(r.winner_side).toUpperCase().trim();
      const wtc = r.winner_team_code == null ? "" : String(r.winner_team_code).toUpperCase().trim();

      if (isFinal) {
        if (wtc === "TIE" || wtc === "DRAW" || ws === "C") winnerSide = "TIE";
        else if (ws === "A" || ws === "B") winnerSide = ws as any;
        else winnerSide = "TIE";
      }

      const buyGross = safeNum(r.buy_gross);
      const sellAmount = safeNum(r.sell_amount);
      const claimAmount = safeNum(r.claim_amount);

      const allInPriceBps =
        r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

      // Return = SELL proceeds + CLAIM payouts
      const returnAmount = Math.max(0, sellAmount + claimAmount);

      // Action
      let action: TradeAggRow["action"] = "Pending";
      if (sellAmount > 0) action = "Sold";
      else if (isFinal && winnerSide === "TIE") action = "Tie";
      else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
        action = winnerSide === side ? "Won" : "Lost";
      } else if (claimAmount > 0 && buyGross > 0) {
        // defensive: claim implies settled payout
        action = "Won";
      }

      // ROI uses total return
      const roi =
        returnAmount > 0 && buyGross > 0 ? (returnAmount - buyGross) / buyGross : null;

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
        claimAmount: claimAmount > 0 ? claimAmount : undefined,

        teamACode,
        teamBCode,

        isFinal,
        winnerSide,

        roi,

        action,
        lastActivityTs: Number(r.last_activity_ts || 0),
      };
    });

    res.json({ ok: true, rows, page, pageSize, totalRows });
  } catch (e: any) {
    console.error("[tradeAggRoutes] error", e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  } finally {
    client.release();
  }
}

tradeAggRoutes.get("/", handleTradeAgg);
tradeAggRoutes.get("/user/:address", handleTradeAgg);

export default tradeAggRoutes;
