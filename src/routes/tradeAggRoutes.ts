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

/**
 * Goal (POSITION-BASED):
 * - Return 1 row per (user, game_id, side) for Trade History.
 *   - If user buys BOTH sides of a game, they will see 2 rows: one for A, one for B.
 * - Aggregate per side:
 *   buyGross + allInPriceBps (weighted by gross) for that side
 *   sellAmount for that side
 * - CLAIM rows are game-level in DB (side='C'). We allocate claimAmount:
 *   - Final winner A/B: claimAmount goes to winning side only
 *   - Final TIE: claimAmount is split pro-rata across A/B rows by buyGross
 *   - If a game has claim but no A/B position rows: return one row with side=null (defensive)
 */
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
    // Count distinct (game_id, side) rows + claim-only rows (side null) inside window
    const countSql = `
      WITH pos AS (
        SELECT
          e.game_id,
          e.side,
          COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type='BUY'), 0)::numeric AS buy_gross,
          COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type='SELL'), 0)::numeric AS sell_amount,
          MAX(e.timestamp)::bigint AS last_activity_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
          AND e.side IN ('A','B')
          AND e.type IN ('BUY','SELL')
        GROUP BY e.game_id, e.side
      ),
      claims AS (
        SELECT
          e.game_id,
          COALESCE(SUM(e.net_out_dec::numeric), 0)::numeric AS claim_amount
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
          AND e.type = 'CLAIM'
        GROUP BY e.game_id
      ),
      claim_only_games AS (
        SELECT c.game_id
        FROM claims c
        LEFT JOIN pos p ON p.game_id = c.game_id
        WHERE p.game_id IS NULL
          AND c.claim_amount > 0
      )
      SELECT COUNT(*)::int AS cnt
      FROM (
        SELECT game_id, side FROM pos
        UNION ALL
        SELECT game_id, NULL::text AS side FROM claim_only_games
      ) x
    `;
    const countRes = await client.query(countSql, params.slice(0, 4));
    const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

    const sql = `
      WITH pos AS (
        SELECT
          e.game_id,
          e.side,

          COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type='BUY'), 0)::numeric AS buy_gross,

          CASE
            WHEN COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type='BUY'), 0) > 0 THEN
              (
                SUM(
                  (COALESCE(e.avg_price_bps, e.spot_price_bps)::numeric)
                  * (e.gross_in_dec::numeric)
                ) FILTER (WHERE e.type='BUY')
                /
                SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type='BUY')
              )
            ELSE NULL
          END AS all_in_price_bps,

          COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type='SELL'), 0)::numeric AS sell_amount,

          MAX(e.timestamp)::bigint AS last_activity_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
          AND e.side IN ('A','B')
          AND e.type IN ('BUY','SELL')
        GROUP BY e.game_id, e.side
      ),

      per_game AS (
        SELECT
          p.*,
          SUM(p.buy_gross) OVER (PARTITION BY p.game_id)::numeric AS total_buy_gross_game
        FROM pos p
      ),

      claims AS (
        SELECT
          e.game_id,
          COALESCE(SUM(e.net_out_dec::numeric), 0)::numeric AS claim_amount,
          MAX(e.timestamp)::bigint AS last_claim_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
          AND e.type = 'CLAIM'
        GROUP BY e.game_id
      ),

      claim_only_games AS (
        SELECT c.game_id, c.claim_amount, c.last_claim_ts
        FROM claims c
        LEFT JOIN per_game p ON p.game_id = c.game_id
        WHERE p.game_id IS NULL
          AND c.claim_amount > 0
      ),

      joined_pos AS (
        SELECT
          p.game_id,
          p.side,
          p.buy_gross,
          p.all_in_price_bps,
          p.sell_amount,
          p.last_activity_ts,
          p.total_buy_gross_game,
          COALESCE(c.claim_amount, 0)::numeric AS claim_amount,
          COALESCE(c.last_claim_ts, 0)::bigint AS last_claim_ts
        FROM per_game p
        LEFT JOIN claims c ON c.game_id = p.game_id
      ),

      unioned AS (
        SELECT
          jp.game_id,
          jp.side,
          jp.buy_gross,
          jp.all_in_price_bps,
          jp.sell_amount,
          jp.last_activity_ts,
          jp.total_buy_gross_game,
          jp.claim_amount,
          jp.last_claim_ts
        FROM joined_pos jp

        UNION ALL

        -- claim-only row (no A/B positions)
        SELECT
          cg.game_id,
          NULL::text AS side,
          0::numeric AS buy_gross,
          NULL::numeric AS all_in_price_bps,
          0::numeric AS sell_amount,
          0::bigint AS last_activity_ts,
          0::numeric AS total_buy_gross_game,
          cg.claim_amount,
          cg.last_claim_ts
        FROM claim_only_games cg
      )

      SELECT
        u.game_id,
        u.side,
        u.buy_gross,
        u.all_in_price_bps,
        u.sell_amount,
        u.last_activity_ts,
        u.total_buy_gross_game,

        u.claim_amount,
        u.last_claim_ts,

        g.league,
        g.lock_time,
        g.is_final,
        g.winner_side,
        g.winner_team_code,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name
      FROM unioned u
      JOIN public.games g ON g.game_id = u.game_id
      ORDER BY GREATEST(u.last_activity_ts, u.last_claim_ts) DESC
      LIMIT $5 OFFSET $6
    `;

    const out = await client.query(sql, params);

    // We allocate claim per row here (winner side only, or pro-rata for tie)
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

      // Winner mapping (same as before)
      let winnerSide: "A" | "B" | "TIE" | null = null;
      const ws =
        r.winner_side == null ? "" : String(r.winner_side).toUpperCase().trim();
      const wtc =
        r.winner_team_code == null
          ? ""
          : String(r.winner_team_code).toUpperCase().trim();

      if (isFinal) {
        if (wtc === "TIE" || wtc === "DRAW" || ws === "C") winnerSide = "TIE";
        else if (ws === "A" || ws === "B") winnerSide = ws as any;
        else winnerSide = "TIE";
      }

      const buyGross = safeNum(r.buy_gross);
      const sellAmount = safeNum(r.sell_amount);

      const allInPriceBps =
        r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

      const claimGame = safeNum(r.claim_amount);
      const totalBuyGrossGame = safeNum(r.total_buy_gross_game);

      // Allocate claim to this row to avoid double-counting:
      let claimAlloc = 0;

      if (claimGame > 0 && isFinal) {
        if (winnerSide === "A" || winnerSide === "B") {
          claimAlloc = side === winnerSide ? claimGame : 0;
        } else if (winnerSide === "TIE") {
          // tie: split pro-rata across A/B rows by buyGross
          claimAlloc =
            totalBuyGrossGame > 0 ? (claimGame * buyGross) / totalBuyGrossGame : 0;
        } else {
          claimAlloc = 0;
        }
      } else if (claimGame > 0 && side == null) {
        // claim-only defensive row
        claimAlloc = claimGame;
      }

      const returnAmount = Math.max(0, sellAmount + claimAlloc);

      let action: TradeAggRow["action"] = "Pending";
      if (sellAmount > 0) action = "Sold";
      else if (isFinal && winnerSide === "TIE") action = "Tie";
      else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
        action = winnerSide === side ? "Won" : "Lost";
      } else if (claimAlloc > 0 && (buyGross > 0 || side == null)) {
        action = "Won";
      }

      const roi = buyGross > 0 ? (returnAmount - buyGross) / buyGross : null;

      const gameLabel =
        teamAName && teamBName
          ? `${teamAName} vs ${teamBName}`
          : teamACode && teamBCode
          ? `${teamACode} vs ${teamBCode}`
          : String(r.game_id);

      // last activity: if claimAlloc is 0, don't let claim timestamp reorder losing side
      const lastActivityTsRaw = safeNum(r.last_activity_ts);
      const lastClaimTsRaw = safeNum(r.last_claim_ts);
      const lastActivityTs =
        claimAlloc > 0 ? Math.max(lastActivityTsRaw, lastClaimTsRaw) : lastActivityTsRaw;

      return {
        gameId: String(r.game_id),
        league: String(r.league || "—"),
        dateTs: Number(r.lock_time || lastActivityTs || 0),
        gameLabel,

        side,
        predictionCode,
        predictionColor: "rgba(230,215,181,0.85)",

        buyGross,
        allInPriceBps,

        returnAmount,
        sellAmount: sellAmount > 0 ? sellAmount : undefined,
        claimAmount: claimAlloc > 0 ? claimAlloc : undefined,

        teamACode,
        teamBCode,

        isFinal,
        winnerSide,

        roi,

        action,
        lastActivityTs: Number(lastActivityTs || 0),
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
