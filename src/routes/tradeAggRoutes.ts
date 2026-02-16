// src/routes/tradeAggRoutes.ts
import { Router } from "express";
import { pool } from "../db";

type MarketType = "GAME" | "PROP";

type TradeAggRow = {
  gameId: string;
  league: string;
  dateTs: number;

  /**
   * GAME markets: "SEA vs NE" etc
   * PROP markets: default to question / short question
   */
  gameLabel: string;

  // ✅ metadata (optional)
  marketType?: MarketType; // "GAME" | "PROP"
  topic?: string;
  marketQuestion?: string;
  marketShort?: string;

  // ✅ canonical key for MULTI (and BINARY via fallback)
  outcomeIndex: number | null;
  outcomeCode: string | null;

  // ✅ keep legacy side for older consumers (derived: 0->A, 1->B else null)
  side: "A" | "B" | null;

  predictionCode: string;
  predictionColor: string;

  buyGross: number;
  allInPriceBps: number | null;

  returnAmount: number;
  claimAmount?: number;
  sellAmount?: number;

  // legacy team fields (still useful for BINARY display)
  teamACode?: string;
  teamBCode?: string;

  isFinal?: boolean;

  // ✅ multi winner (optional)
  winningOutcomeIndex?: number | null;
  winningOutcomeCode?: string | null;

  // legacy winnerSide for old UI (BINARY only; null for MULTI)
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

function safeStr(v: any): string {
  return v == null ? "" : String(v);
}

function normMarketType(v: any): MarketType | undefined {
  const s = safeStr(v).toUpperCase().trim();
  if (s === "PROP") return "PROP";
  if (s === "GAME") return "GAME";
  return undefined;
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
    const countSql = `
      WITH pos AS (
        SELECT
          e.game_id,

          COALESCE(
            e.outcome_index,
            CASE
              WHEN e.side='A' THEN 0
              WHEN e.side='B' THEN 1
              WHEN e.side='C' THEN 2 -- ✅ DRAW legacy (BUY/SELL only; CLAIM is filtered out)
              ELSE NULL
            END
          ) AS outcome_index,

          COALESCE(
            e.outcome_code,
            CASE
              WHEN e.outcome_index IS NOT NULL THEN e.outcome_code
              WHEN e.side='A' THEN g.team_a_code
              WHEN e.side='B' THEN g.team_b_code
              WHEN e.side='C' THEN 'DRAW'
              ELSE NULL
            END
          ) AS outcome_code,

          COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type='BUY'), 0)::numeric AS buy_gross,
          COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type='SELL'), 0)::numeric AS sell_amount,
          MAX(e.timestamp)::bigint AS last_activity_ts
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
          AND e.type IN ('BUY','SELL')
          AND (
            e.outcome_index IS NOT NULL
            OR e.side IN ('A','B','C') -- ✅ include legacy DRAW in positions
          )
        GROUP BY
          e.game_id,
          COALESCE(
            e.outcome_index,
            CASE
              WHEN e.side='A' THEN 0
              WHEN e.side='B' THEN 1
              WHEN e.side='C' THEN 2
              ELSE NULL
            END
          ),
          COALESCE(
            e.outcome_code,
            CASE
              WHEN e.outcome_index IS NOT NULL THEN e.outcome_code
              WHEN e.side='A' THEN g.team_a_code
              WHEN e.side='B' THEN g.team_b_code
              WHEN e.side='C' THEN 'DRAW'
              ELSE NULL
            END
          )
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
        SELECT game_id, outcome_index FROM pos
        UNION ALL
        SELECT game_id, NULL::int AS outcome_index FROM claim_only_games
      ) x
    `;
    const countRes = await client.query(countSql, params.slice(0, 4));
    const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

    const sql = `
      WITH pos AS (
        SELECT
          e.game_id,

          COALESCE(
            e.outcome_index,
            CASE
              WHEN e.side='A' THEN 0
              WHEN e.side='B' THEN 1
              WHEN e.side='C' THEN 2 -- ✅ DRAW legacy (BUY/SELL only)
              ELSE NULL
            END
          ) AS outcome_index,

          COALESCE(
            e.outcome_code,
            CASE
              WHEN e.outcome_index IS NOT NULL THEN e.outcome_code
              WHEN e.side='A' THEN g.team_a_code
              WHEN e.side='B' THEN g.team_b_code
              WHEN e.side='C' THEN 'DRAW'
              ELSE NULL
            END
          ) AS outcome_code,

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
          AND e.type IN ('BUY','SELL')
          AND (
            e.outcome_index IS NOT NULL
            OR e.side IN ('A','B','C') -- ✅ include legacy DRAW in positions
          )
        GROUP BY
          e.game_id,
          COALESCE(
            e.outcome_index,
            CASE
              WHEN e.side='A' THEN 0
              WHEN e.side='B' THEN 1
              WHEN e.side='C' THEN 2
              ELSE NULL
            END
          ),
          COALESCE(
            e.outcome_code,
            CASE
              WHEN e.outcome_index IS NOT NULL THEN e.outcome_code
              WHEN e.side='A' THEN g.team_a_code
              WHEN e.side='B' THEN g.team_b_code
              WHEN e.side='C' THEN 'DRAW'
              ELSE NULL
            END
          )
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
          p.outcome_index,
          p.outcome_code,
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
          jp.outcome_index,
          jp.outcome_code,
          jp.buy_gross,
          jp.all_in_price_bps,
          jp.sell_amount,
          jp.last_activity_ts,
          jp.total_buy_gross_game,
          jp.claim_amount,
          jp.last_claim_ts
        FROM joined_pos jp

        UNION ALL

        SELECT
          cg.game_id,
          NULL::int AS outcome_index,
          NULL::text AS outcome_code,
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
        u.outcome_index,
        u.outcome_code,
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

        -- legacy binary winner fields
        g.winner_side,
        g.winner_team_code,

        -- ✅ multi winner index (games has index; NOT necessarily a code column)
        g.winning_outcome_index,

        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name,

        -- ✅ PROP FIELDS
        g.market_type,
        g.topic,
        g.market_question,
        g.market_short

      FROM unioned u
      JOIN public.games g ON g.game_id = u.game_id
      ORDER BY GREATEST(u.last_activity_ts, u.last_claim_ts) DESC
      LIMIT $5 OFFSET $6
    `;

    const out = await client.query(sql, params);

    const rows: TradeAggRow[] = (out.rows || []).map((r: any) => {
      const teamACode = r.team_a_code ? String(r.team_a_code) : undefined;
      const teamBCode = r.team_b_code ? String(r.team_b_code) : undefined;
      const teamAName = r.team_a_name ? String(r.team_a_name) : "";
      const teamBName = r.team_b_name ? String(r.team_b_name) : "";

      const marketType = normMarketType(r.market_type);
      const topic = safeStr(r.topic).trim() || undefined;
      const marketQuestion = safeStr(r.market_question).trim() || undefined;
      const marketShort = safeStr(r.market_short).trim() || undefined;

      const outcomeIndex =
        r.outcome_index != null
          ? Number(r.outcome_index)
          : r.side === "A"
            ? 0
            : r.side === "B"
              ? 1
              : r.side === "C"
                ? 2
                : null;

      const outcomeCode =
        r.outcome_code != null
          ? String(r.outcome_code)
          : outcomeIndex === 0
            ? teamACode ?? null
            : outcomeIndex === 1
              ? teamBCode ?? null
              : outcomeIndex === 2
                ? "DRAW"
                : null;

      const side: "A" | "B" | null = outcomeIndex === 0 ? "A" : outcomeIndex === 1 ? "B" : null;

      const predictionCode = outcomeCode || side || "—";

      const isFinal = r.is_final == null ? undefined : Boolean(r.is_final);

      // ✅ multi winner (index only; code inferred for 0/1/2)
      const winningOutcomeIndex =
        r.winning_outcome_index == null ? null : Number(r.winning_outcome_index);

      const winningOutcomeCode =
        winningOutcomeIndex == null
          ? null
          : winningOutcomeIndex === 0
            ? (teamACode ?? null)
            : winningOutcomeIndex === 1
              ? (teamBCode ?? null)
              : winningOutcomeIndex === 2
                ? "DRAW"
                : null;

      // legacy binary winner
      let winnerSide: "A" | "B" | "TIE" | null = null;
      const ws = r.winner_side == null ? "" : String(r.winner_side).toUpperCase().trim();
      const wtc = r.winner_team_code == null ? "" : String(r.winner_team_code).toUpperCase().trim();

      if (isFinal && winningOutcomeIndex == null) {
        if (wtc === "TIE" || wtc === "DRAW" || ws === "C") winnerSide = "TIE";
        else if (ws === "A" || ws === "B") winnerSide = ws as any;
        else winnerSide = "TIE";
      }

      const buyGross = safeNum(r.buy_gross);
      const sellAmount = safeNum(r.sell_amount);

      const allInPriceBps = r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

      const claimGame = safeNum(r.claim_amount);
      const totalBuyGrossGame = safeNum(r.total_buy_gross_game);

      let claimAlloc = 0;

      if (claimGame > 0 && isFinal) {
        if (winningOutcomeIndex != null) {
          // MULTI/3-way: claim only to winning outcome index
          claimAlloc = outcomeIndex === winningOutcomeIndex ? claimGame : 0;
        } else if (winnerSide === "A" || winnerSide === "B") {
          // BINARY legacy
          claimAlloc = side === winnerSide ? claimGame : 0;
        } else if (winnerSide === "TIE") {
          // BINARY legacy tie: split pro-rata across A/B rows by buyGross
          claimAlloc = totalBuyGrossGame > 0 ? (claimGame * buyGross) / totalBuyGrossGame : 0;
        } else {
          claimAlloc = 0;
        }
      } else if (claimGame > 0 && outcomeIndex == null) {
        // claim-only defensive row
        claimAlloc = claimGame;
      }

      const returnAmount = Math.max(0, sellAmount + claimAlloc);

      let action: TradeAggRow["action"] = "Pending";
      if (sellAmount > 0) action = "Sold";
      else if (isFinal && winningOutcomeIndex != null) {
        if (outcomeIndex != null && outcomeIndex === winningOutcomeIndex) action = "Won";
        else if (outcomeIndex != null) action = "Lost";
        else action = "Won";
      } else if (isFinal && winnerSide === "TIE") action = "Tie";
      else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
        action = winnerSide === side ? "Won" : "Lost";
      } else if (claimAlloc > 0 && (buyGross > 0 || outcomeIndex == null)) {
        action = "Won";
      }

      const roi = buyGross > 0 ? (returnAmount - buyGross) / buyGross : null;

      const defaultGameLabel =
        teamAName && teamBName
          ? `${teamAName} vs ${teamBName}`
          : teamACode && teamBCode
            ? `${teamACode} vs ${teamBCode}`
            : String(r.game_id);

      const gameLabel =
        marketType === "PROP" ? (marketShort || marketQuestion || topic || defaultGameLabel) : defaultGameLabel;

      const lastActivityTsRaw = safeNum(r.last_activity_ts);
      const lastClaimTsRaw = safeNum(r.last_claim_ts);
      const lastActivityTs = claimAlloc > 0 ? Math.max(lastActivityTsRaw, lastClaimTsRaw) : lastActivityTsRaw;

      return {
        gameId: String(r.game_id),
        league: String(r.league || "—"),
        dateTs: Number(r.lock_time || lastActivityTs || 0),
        gameLabel,

        marketType,
        topic,
        marketQuestion,
        marketShort,

        outcomeIndex: outcomeIndex == null ? null : outcomeIndex,
        outcomeCode: outcomeCode,

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

        winningOutcomeIndex,
        winningOutcomeCode,

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
