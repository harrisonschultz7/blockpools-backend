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

function toWinnerSide(isFinal: boolean | undefined, winnerSideRaw: any, winnerTeamCodeRaw: any) {
  if (!isFinal) return null;

  const ws =
    winnerSideRaw == null ? "" : String(winnerSideRaw).toUpperCase().trim();
  const wtc =
    winnerTeamCodeRaw == null ? "" : String(winnerTeamCodeRaw).toUpperCase().trim();

  if (wtc === "TIE" || wtc === "DRAW" || ws === "C") return "TIE" as const;
  if (ws === "A" || ws === "B") return ws as "A" | "B";
  // final but missing winner => treat as tie (defensive)
  return "TIE" as const;
}

export const tradeAggRoutes = Router();

/**
 * mode:
 * - "game" (default): EXACTLY 1 row per game (best_side) [legacy behavior]
 * - "position": 1 row per (game_id, side) so a user can have BOTH sides show up
 *
 * IMPORTANT:
 * - CLAIM rows are stored as side='C' (game-level). For "position" mode:
 *   - If winner is A/B => assign claim_amount only to that winner side row
 *   - If tie => pro-rate claim across A/B rows by buy_gross
 */
async function handleTradeAgg(req: any, res: any) {
  const address = normAddr(String(req.query.user || req.params.address || ""));
  if (!assertAddr(address)) {
    return res.status(400).json({ ok: false, error: "Invalid address" });
  }

  const league = String(req.query.league || "ALL").toUpperCase().trim();
  const range = String(req.query.range || "ALL").toUpperCase().trim();

  const mode = String(req.query.mode || "game").toLowerCase().trim(); // "game" | "position"

  const page = clampPage(req.query.page);
  const pageSize = clampPageSize(req.query.pageSize);
  const offset = (page - 1) * pageSize;

  const { start, end } = rangeToWindow(range);

  // params: $1 addr, $2 start, $3 end, $4 league, $5 limit, $6 offset
  const params = [address, start, end, league, pageSize, offset];

  const client = await pool.connect();
  try {
    if (mode === "position") {
      // ---------- POSITION MODE (game_id + side) ----------
      // Count distinct (game_id, side) from A/B position activity, plus claim-only games as side NULL.
      const countSql = `
        WITH pos_sides AS (
          SELECT e.game_id, e.side
          FROM public.user_trade_events e
          JOIN public.games g ON g.game_id = e.game_id
          WHERE lower(e.user_address) = lower($1)
            AND g.lock_time >= $2 AND g.lock_time <= $3
            AND ($4 = 'ALL' OR g.league = $4)
            AND e.side IN ('A','B')
            AND e.type IN ('BUY','SELL')
          GROUP BY e.game_id, e.side
        ),
        claim_only AS (
          SELECT e.game_id
          FROM public.user_trade_events e
          JOIN public.games g ON g.game_id = e.game_id
          WHERE lower(e.user_address) = lower($1)
            AND g.lock_time >= $2 AND g.lock_time <= $3
            AND ($4 = 'ALL' OR g.league = $4)
            AND e.type = 'CLAIM'
          GROUP BY e.game_id
        )
        SELECT (
          (SELECT COUNT(*)::int FROM pos_sides)
          +
          (SELECT COUNT(*)::int
             FROM claim_only c
             WHERE NOT EXISTS (SELECT 1 FROM pos_sides p WHERE p.game_id = c.game_id)
          )
        ) AS cnt
      `;
      const countRes = await client.query(countSql, params.slice(0, 4));
      const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

      const sql = `
        WITH pos AS (
          SELECT
            e.game_id,
            e.side,

            COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)::numeric AS buy_gross,

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

            COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type = 'SELL'), 0)::numeric AS sell_amount,

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

        pos_totals AS (
          SELECT
            game_id,
            COALESCE(SUM(buy_gross),0)::numeric AS game_buy_total
          FROM pos
          GROUP BY game_id
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

        claim_only AS (
          -- games with CLAIM but no A/B position rows
          SELECT c.game_id
          FROM claims c
          WHERE NOT EXISTS (SELECT 1 FROM pos p WHERE p.game_id = c.game_id)
        ),

        merged AS (
          -- A/B position rows
          SELECT
            p.game_id,
            p.side,
            p.buy_gross,
            p.all_in_price_bps,
            p.sell_amount,
            COALESCE(c.claim_amount, 0)::numeric AS claim_amount_total,
            COALESCE(t.game_buy_total, 0)::numeric AS game_buy_total,
            GREATEST(COALESCE(p.last_activity_ts,0), COALESCE(c.last_claim_ts,0))::bigint AS last_activity_ts
          FROM pos p
          LEFT JOIN claims c ON c.game_id = p.game_id
          LEFT JOIN pos_totals t ON t.game_id = p.game_id

          UNION ALL

          -- claim-only rows => side NULL (we will render as null)
          SELECT
            co.game_id,
            NULL::text AS side,
            0::numeric AS buy_gross,
            NULL::numeric AS all_in_price_bps,
            0::numeric AS sell_amount,
            COALESCE(c.claim_amount,0)::numeric AS claim_amount_total,
            0::numeric AS game_buy_total,
            COALESCE(c.last_claim_ts,0)::bigint AS last_activity_ts
          FROM claim_only co
          JOIN claims c ON c.game_id = co.game_id
        )

        SELECT
          m.game_id,
          m.side,
          m.buy_gross,
          m.all_in_price_bps,
          m.sell_amount,
          m.claim_amount_total,
          m.game_buy_total,
          m.last_activity_ts,

          g.league,
          g.lock_time,
          g.is_final,
          g.winner_side,
          g.winner_team_code,
          g.team_a_code,
          g.team_b_code,
          g.team_a_name,
          g.team_b_name
        FROM merged m
        JOIN public.games g ON g.game_id = m.game_id
        ORDER BY m.last_activity_ts DESC
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
        const winnerSide = toWinnerSide(isFinal, r.winner_side, r.winner_team_code);

        const buyGross = safeNum(r.buy_gross);
        const sellAmount = safeNum(r.sell_amount);

        const claimTotal = safeNum(r.claim_amount_total);
        const gameBuyTotal = safeNum(r.game_buy_total);

        // Allocate claim to a position row:
        // - winner A/B => allocate to that side only
        // - tie => pro-rate by buyGross
        // - claim-only row (side null) => keep full claim on that row
        let claimAmountAllocated = 0;

        if (!side) {
          claimAmountAllocated = claimTotal;
        } else if (isFinal && (winnerSide === "A" || winnerSide === "B")) {
          claimAmountAllocated = winnerSide === side ? claimTotal : 0;
        } else if (isFinal && winnerSide === "TIE") {
          if (claimTotal > 0 && gameBuyTotal > 0 && buyGross > 0) {
            claimAmountAllocated = (claimTotal * buyGross) / gameBuyTotal;
          } else {
            claimAmountAllocated = 0;
          }
        } else {
          // not final => no claim expected, but keep defensive behavior
          claimAmountAllocated = 0;
        }

        const allInPriceBps =
          r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

        const returnAmount = Math.max(0, sellAmount + claimAmountAllocated);

        let action: TradeAggRow["action"] = "Pending";
        if (sellAmount > 0) action = "Sold";
        else if (isFinal && winnerSide === "TIE") action = "Tie";
        else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
          action = winnerSide === side ? "Won" : "Lost";
        } else if (!side && claimAmountAllocated > 0 && isFinal) {
          // claim-only row: settled payout/refund
          action = winnerSide === "TIE" ? "Tie" : "Won";
        }

        const roi = buyGross > 0 ? (returnAmount - buyGross) / buyGross : null;

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
          claimAmount: claimAmountAllocated > 0 ? claimAmountAllocated : undefined,

          teamACode,
          teamBCode,

          isFinal,
          winnerSide,

          roi,

          action,
          lastActivityTs: Number(r.last_activity_ts || 0),
        };
      });

      return res.json({ ok: true, mode: "position", rows, page, pageSize, totalRows });
    }

    // ---------- GAME MODE (legacy behavior, your original approach) ----------
    // Count distinct games within window
    const countSql = `
      SELECT COUNT(*)::int AS cnt
      FROM (
        SELECT e.game_id
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
        GROUP BY e.game_id
      ) x
    `;
    const countRes = await client.query(countSql, params.slice(0, 4));
    const totalRows = Number(countRes.rows?.[0]?.cnt || 0);

    const sql = `
      WITH pos AS (
        SELECT
          e.game_id,
          e.side,

          COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)::numeric AS buy_gross,

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

          COALESCE(SUM(e.net_out_dec::numeric) FILTER (WHERE e.type = 'SELL'), 0)::numeric AS sell_amount,

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

      best_side AS (
        SELECT DISTINCT ON (p.game_id)
          p.game_id,
          p.side,
          p.buy_gross,
          p.all_in_price_bps,
          p.sell_amount,
          p.last_activity_ts
        FROM pos p
        ORDER BY
          p.game_id,
          p.buy_gross DESC,
          p.last_activity_ts DESC,
          p.side ASC
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

      touched_games AS (
        SELECT DISTINCT e.game_id
        FROM public.user_trade_events e
        JOIN public.games g ON g.game_id = e.game_id
        WHERE lower(e.user_address) = lower($1)
          AND g.lock_time >= $2 AND g.lock_time <= $3
          AND ($4 = 'ALL' OR g.league = $4)
      ),

      merged AS (
        SELECT
          tg.game_id,

          bs.side,
          COALESCE(bs.buy_gross, 0)::numeric AS buy_gross,
          bs.all_in_price_bps,
          COALESCE(bs.sell_amount, 0)::numeric AS sell_amount,
          COALESCE(c.claim_amount, 0)::numeric AS claim_amount,

          GREATEST(
            COALESCE(bs.last_activity_ts, 0),
            COALESCE(c.last_claim_ts, 0)
          )::bigint AS last_activity_ts
        FROM touched_games tg
        LEFT JOIN best_side bs ON bs.game_id = tg.game_id
        LEFT JOIN claims c ON c.game_id = tg.game_id
      )

      SELECT
        m.game_id,
        m.side,
        m.buy_gross,
        m.all_in_price_bps,
        m.sell_amount,
        m.claim_amount,
        m.last_activity_ts,

        g.league,
        g.lock_time,
        g.is_final,
        g.winner_side,
        g.winner_team_code,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name
      FROM merged m
      JOIN public.games g ON g.game_id = m.game_id
      ORDER BY m.last_activity_ts DESC
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
      const winnerSide = toWinnerSide(isFinal, r.winner_side, r.winner_team_code);

      const buyGross = safeNum(r.buy_gross);
      const sellAmount = safeNum(r.sell_amount);
      const claimAmount = safeNum(r.claim_amount);

      const allInPriceBps =
        r.all_in_price_bps == null ? null : Math.round(Number(r.all_in_price_bps));

      const returnAmount = Math.max(0, sellAmount + claimAmount);

      let action: TradeAggRow["action"] = "Pending";
      if (sellAmount > 0) action = "Sold";
      else if (isFinal && winnerSide === "TIE") action = "Tie";
      else if (isFinal && side && (winnerSide === "A" || winnerSide === "B")) {
        action = winnerSide === side ? "Won" : "Lost";
      } else if (claimAmount > 0 && buyGross > 0) {
        action = "Won";
      }

      const roi = buyGross > 0 ? (returnAmount - buyGross) / buyGross : null;

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

    res.json({ ok: true, mode: "game", rows, page, pageSize, totalRows });
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
