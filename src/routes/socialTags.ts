// src/routes/socialTags.ts
//
// Social Tags — anonymous "what kind of bettors are on this market" badges.
//
// Two responsibilities live here:
//   1. POST /api/social-tags/refresh  (cron) — recompute the materialized
//      user_tags_by_league table (🔥 Hot / 🔮 Sharp). See migration
//      2026-06-23_user_social_tags.sql for the data model rationale.
//   2. GET  /api/social-tags?gameIds=… (read) — per-market, per-side counts of
//      🐋 Whale / 🔥 Hot / 🔮 Sharp bettors. (Added in the next build step.)
//
// Tags are mutually exclusive at render time with priority 🐋 Whale > 🔥 Hot >
// 🔮 Sharp, so each bettor contributes exactly one badge. Whale is a per-position
// property ($30+ of own money on that side) computed live; Hot/Sharp are
// per-(user, league) properties materialized by the refresh below.
//
// Attribution uses effective_user_address (= COALESCE(beneficiary_address,
// user_address)) so promo/free-bet trades count toward the real beneficiary —
// matching masterMetrics.ts / the profile page / tradeAggRoutes.ts.

import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

// ── Tunable thresholds ───────────────────────────────────────────────────────
// Launch values are intentionally loose: at current volume, strict thresholds
// (3-in-a-row / min 5 settled) tag almost nobody. Tighten these as history
// deepens — a change here + one cron run is all it takes (no schema change).
const ROI_WINDOW_DAYS  = 30;  // Sharp ROI lookback window
const HOT_STREAK_LEN   = 2;   // 🔥 Hot: the N most-recent resolved picks were all wins
const SHARP_ROI_PCT    = 10;  // 🔮 Sharp: 30-day ROI must be >= this (percent)
const SHARP_MIN_TRADES = 3;   // 🔮 Sharp: minimum settled BUY trades in the window
const WHALE_MIN_USD    = 30;  // 🐋 Whale: own (non-promo) money bet on a side, in USD
const OPEN_TTL_MS      = 60_000; // open markets: per-side counts cached this long

// ── POST /api/social-tags/refresh ────────────────────────────────────────────
// Full rebuild of user_tags_by_league inside a transaction. The table only holds
// tagged users (a few hundred rows at most), so a wipe-and-rebuild is simpler and
// safer than incremental upserts (no stale rows to garbage-collect).
//
// hot   = the user's HOT_STREAK_LEN most-recent RESOLVED picks in the league were
//         all wins. A "pick" per game is the outcome the user staked the most on.
// sharp = 30-day ROI >= SHARP_ROI_PCT with >= SHARP_MIN_TRADES settled trades.
//         Mirrors computeLiveRoi() in leagueChat.ts exactly, but grouped by
//         (effective_user_address, league).
// A user qualifying for both is stored as 'hot' (Hot > Sharp); Whale is layered
// on per-position at read time.

const REBUILD_SQL = `
  WITH picks AS (
    -- per (user, league, game): the outcome the user staked the most on,
    -- among final + resolved games with a known winner. last_ts = the user's
    -- most recent trade on that game; it orders the streak below.
    --
    -- NOTE: we deliberately order recency by the user's trade time, NOT by
    -- games.lock_time. Futures / MULTI markets (e.g. World Cup group/tournament
    -- winners) carry a sentinel lock_time of 9999999999, so ordering by lock_time
    -- would make every futures market sort as "most recent" and scramble the
    -- streak. Trade timestamp is a real time on every market type.
    SELECT addr, league, game_id, last_ts, outcome_index, winning_outcome_index,
           ROW_NUMBER() OVER (PARTITION BY addr, game_id ORDER BY staked DESC, outcome_index ASC) AS rn
    FROM (
      SELECT LOWER(e.effective_user_address) AS addr,
             g.league                        AS league,
             e.game_id                       AS game_id,
             e.outcome_index                 AS outcome_index,
             g.winning_outcome_index         AS winning_outcome_index,
             SUM(COALESCE(e.gross_in_dec, 0)) AS staked,
             MAX(e.timestamp)                AS last_ts
      FROM public.user_trade_events e
      JOIN public.games g ON g.game_id = e.game_id
      WHERE e.type = 'BUY'
        AND e.outcome_index IS NOT NULL
        AND g.is_final = true
        AND g.resolution_type IN ('NORMAL', 'RESOLVED')
        AND g.winning_outcome_index IS NOT NULL
      GROUP BY 1, 2, 3, 4, 5
    ) s
  ),
  seq AS (
    SELECT addr, league,
           (outcome_index = winning_outcome_index) AS won,
           ROW_NUMBER() OVER (PARTITION BY addr, league ORDER BY last_ts DESC NULLS LAST, game_id DESC) AS rn2
    FROM picks
    WHERE rn = 1
  ),
  hot AS (
    SELECT addr, league
    FROM seq
    WHERE rn2 <= $2
    GROUP BY addr, league
    HAVING COUNT(*) = $2 AND bool_and(won)
  ),
  filtered AS (
    SELECT LOWER(e.effective_user_address) AS addr,
           g.league AS league,
           e.type, g.is_final, g.resolution_type,
           COALESCE(e.gross_in_dec, 0)          AS gross_in,
           COALESCE(e.net_out_dec, 0)           AS net_out,
           COALESCE(e.cost_basis_closed_dec, 0) AS cost_basis_closed
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE e.timestamp >= $1
  ),
  agg AS (
    SELECT addr, league,
      ( COALESCE(SUM(gross_in)          FILTER (WHERE type = 'BUY'  AND is_final = true  AND resolution_type IN ('NORMAL', 'RESOLVED')), 0)
        + COALESCE(SUM(cost_basis_closed) FILTER (WHERE type = 'SELL' AND is_final = false), 0)
      ) AS total_traded,
      COALESCE(SUM(net_out) FILTER (WHERE type IN ('SELL', 'CLAIM')), 0) AS total_return,
      COUNT(*) FILTER (WHERE type = 'BUY' AND is_final = true)           AS trades_settled
    FROM filtered
    GROUP BY addr, league
  ),
  sharp AS (
    SELECT addr, league
    FROM agg
    WHERE total_traded > 0
      AND (total_return / total_traded - 1) * 100 >= $3
      AND trades_settled >= $4
  )
  INSERT INTO public.user_tags_by_league (user_address, league, tag, computed_at)
  SELECT COALESCE(h.addr, s.addr),
         COALESCE(h.league, s.league),
         CASE WHEN h.addr IS NOT NULL THEN 'hot' ELSE 'sharp' END,
         now()
  FROM hot h
  FULL OUTER JOIN sharp s ON h.addr = s.addr AND h.league = s.league
`;

router.post("/refresh", async (req: Request, res: Response) => {
  const token = req.headers.authorization?.replace("Bearer ", "");
  if (token !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const windowSec = Math.floor(Date.now() / 1000) - ROI_WINDOW_DAYS * 86400;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("DELETE FROM public.user_tags_by_league");
    await client.query(REBUILD_SQL, [
      windowSec,
      HOT_STREAK_LEN,
      SHARP_ROI_PCT,
      SHARP_MIN_TRADES,
    ]);
    const { rows } = await client.query(
      "SELECT tag, COUNT(*)::int AS n FROM public.user_tags_by_league GROUP BY tag"
    );
    await client.query("COMMIT");

    const counts = { hot: 0, sharp: 0 };
    for (const r of rows) {
      if (r.tag === "hot") counts.hot = r.n;
      if (r.tag === "sharp") counts.sharp = r.n;
    }
    return res.json({
      ok: true,
      counts,
      thresholds: {
        hot_streak_len: HOT_STREAK_LEN,
        sharp_roi_pct: SHARP_ROI_PCT,
        sharp_min_trades: SHARP_MIN_TRADES,
        roi_window_days: ROI_WINDOW_DAYS,
      },
      refreshed_at: new Date().toISOString(),
    });
  } catch (err: any) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[social-tags] refresh failed:", err?.message || err);
    return res.status(500).json({ error: "refresh_failed", detail: String(err?.message || err) });
  } finally {
    client.release();
  }
});

// ── GET /api/social-tags?gameIds=0x..,0x.. ───────────────────────────────────
// Per-market, per-side counts of 🐋 Whale / 🔥 Hot / 🔮 Sharp bettors.
//
// "On a side" = holds a net BUY position > 0 on that outcome (someone who bought
// then fully sold out is excluded). NOTE the NULL trap: SUM(...) FILTER(...) is
// NULL when no rows match, and `x - NULL = NULL`, so each aggregate is wrapped in
// its own COALESCE — not just the inner value.
//
// 🐋 Whale = $30+ of OWN (non-promo) money bet on the side. Promo/free-bet stake
// is excluded (it's house money) but a promo bettor still counts toward Hot/Sharp.
//
// Each holder collapses to ONE badge by priority 🐋 Whale > 🔥 Hot > 🔮 Sharp, so
// counts sum to the number of distinct tagged bettors. Untagged holders are not
// counted. Output: { [gameId]: { [outcomeIndex]: { whale, hot, sharp } } }.

const AGG_SQL = `
  WITH pos AS (
    SELECT e.game_id, e.outcome_index, LOWER(e.effective_user_address) AS addr,
           COALESCE(SUM(COALESCE(e.net_stake_dec, 0))         FILTER (WHERE e.type = 'BUY'), 0)
             - COALESCE(SUM(COALESCE(e.cost_basis_closed_dec, 0)) FILTER (WHERE e.type = 'SELL'), 0) AS net_remaining,
           COALESCE(SUM(COALESCE(e.gross_in_dec, 0))
             FILTER (WHERE e.type = 'BUY' AND e.promo_redemption_id IS NULL), 0) AS own_gross
    FROM public.user_trade_events e
    WHERE e.game_id = ANY($1::text[])
      AND e.outcome_index IS NOT NULL
    GROUP BY e.game_id, e.outcome_index, LOWER(e.effective_user_address)
  ),
  holders AS (
    SELECT p.game_id, p.outcome_index, p.addr, (p.own_gross >= $2) AS is_whale
    FROM pos p
    WHERE p.net_remaining > 0
  ),
  tagged AS (
    SELECT h.game_id, h.outcome_index,
      CASE
        WHEN h.is_whale   THEN 'whale'
        WHEN t.tag = 'hot'   THEN 'hot'
        WHEN t.tag = 'sharp' THEN 'sharp'
        ELSE NULL
      END AS final_tag
    FROM holders h
    JOIN public.games g ON g.game_id = h.game_id
    LEFT JOIN public.user_tags_by_league t
      ON t.user_address = h.addr AND t.league = g.league
  )
  SELECT game_id, outcome_index, final_tag, COUNT(*)::int AS n
  FROM tagged
  WHERE final_tag IS NOT NULL
  GROUP BY game_id, outcome_index, final_tag
`;

type SideCounts = { whale: number; hot: number; sharp: number };
type GameTags = Record<string, SideCounts>; // outcomeIndex -> counts
type CacheEntry = { data: GameTags; frozen: boolean; expiresAt: number };

// Per-game cache. Once a market locks/settles its counts can never change, so we
// freeze them forever; open markets get a short TTL and also get invalidated
// on-trade (see invalidateSocialTags, wired into cacheRefresh).
const tagCache = new Map<string, CacheEntry>();

/** Drop cached counts for a game (or all) so the next read recomputes. Called
 *  from the post-trade cache refresh so live counts update within seconds. */
export function invalidateSocialTags(gameId?: string): void {
  if (gameId) tagCache.delete(gameId.toLowerCase());
  else tagCache.clear();
}

router.get("/", async (req: Request, res: Response) => {
  const raw = String(req.query.gameIds || "").trim();
  if (!raw) return res.json({});

  const ids = Array.from(
    new Set(raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean))
  ).slice(0, 200); // cap fan-out
  if (!ids.length) return res.json({});

  const now = Date.now();
  const nowSec = Math.floor(now / 1000);
  const result: Record<string, GameTags> = {};
  const toFetch: string[] = [];

  for (const id of ids) {
    const c = tagCache.get(id);
    if (c && (c.frozen || c.expiresAt > now)) result[id] = c.data;
    else toFetch.push(id);
  }

  if (toFetch.length) {
    try {
      const [aggRes, metaRes] = await Promise.all([
        pool.query(AGG_SQL, [toFetch, WHALE_MIN_USD]),
        pool.query(
          "SELECT game_id, lock_time, is_final FROM public.games WHERE game_id = ANY($1::text[])",
          [toFetch]
        ),
      ]);

      const fetched: Record<string, GameTags> = {};
      for (const id of toFetch) fetched[id] = {};
      for (const row of aggRes.rows) {
        const gid = String(row.game_id);
        const oi = String(row.outcome_index);
        if (!fetched[gid]) fetched[gid] = {};
        if (!fetched[gid][oi]) fetched[gid][oi] = { whale: 0, hot: 0, sharp: 0 };
        (fetched[gid][oi] as any)[row.final_tag] = row.n;
      }

      const meta = new Map<string, { lock: number | null; final: boolean }>();
      for (const m of metaRes.rows) {
        meta.set(String(m.game_id), {
          lock: m.lock_time != null ? Number(m.lock_time) : null,
          final: !!m.is_final,
        });
      }

      for (const id of toFetch) {
        const m = meta.get(id);
        const frozen = !!m && (m.final || (m.lock != null && m.lock <= nowSec));
        tagCache.set(id, { data: fetched[id], frozen, expiresAt: now + OPEN_TTL_MS });
        result[id] = fetched[id];
      }
    } catch (err: any) {
      console.error("[social-tags] read failed:", err?.message || err);
      // Best effort: serve whatever was cached; fill the rest empty so the
      // frontend simply renders no badges rather than erroring.
      for (const id of toFetch) if (!(id in result)) result[id] = {};
    }
  }

  return res.json(result);
});

export default router;
