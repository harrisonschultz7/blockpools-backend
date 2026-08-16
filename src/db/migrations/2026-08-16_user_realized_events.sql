-- ============================================================================
-- Canonical realized-ROI ledger: one row per REALIZED cash event per user, with
-- the buy cost basis matched to it (cohort/average-cost matching).
--
-- ALREADY APPLIED to Supabase on 2026-08-16 (via MCP apply_migration:
-- user_realized_events + user_realized_events_settle_ts_fix). Kept here as the
-- canonical definition; idempotent (safe to re-run in the SQL editor).
--
-- Why: the old 30d ROI formula ((SELL+CLAIM net_out in window) / (BUYs on FINAL
-- games in window) - 1) had three inflation bugs:
--   1. Window asymmetry — a claim inside the window whose stake was placed just
--      outside it counts the payout with NO matching stake (observed: a wallet
--      showing 1,098% 30d ROI whose real realized ROI was +79.6%).
--   2. SELL proceeds counted against cost_basis_closed_dec, which is 0 on all
--      v2 (order-book) SELL rows — merge round-trips looked like pure return.
--   3. Losses only realize implicitly (stake with no payout), so timing was
--      arbitrary and open-game stakes dragged the denominator.
--
-- Model ("realized ROI"): a return only ever appears WITH the cost that
-- produced it, attributed to the period the position CLOSED:
--   * SELL  — realizes at the sell timestamp. Cost = shares sold x average buy
--             cost/share of that (user, game, outcome) position. Clamped so
--             cumulative matched cost never exceeds total buy cost.
--   * CLAIM — realizes at the (last) claim timestamp. Cost = the not-yet-sold
--             buy cost of the WINNING outcome (or of ALL outcomes for
--             tie/void/refund resolutions, where the claim is a refund).
--   * LOSS  — a final NORMAL/RESOLVED game realizes the remaining (unsold) buy
--             cost of each LOSING outcome as a zero-return event, timestamped
--             at the FIRST CLAIM on that game by ANY user (winners claim right
--             after settlement, so this is the best available settlement
--             signal: games.updated_at is not reliably bumped at finalization,
--             and lock_time is a year-2286 sentinel on futures/MULTI pools).
--             Falls back to lock_time (when real), then updated_at.
--   * Winning shares never claimed, and positions on open games, stay
--     UNREALIZED — they appear nowhere in this view.
--   * Promo free-bet events (promo_redemption_id IS NOT NULL) are excluded
--     entirely: the user staked nothing, so including them recreates the
--     infinite-ROI bug (62 promo claims exist with no user-side BUY).
--
-- Shares are reconstructed as amount * 10000 / avg_price_bps (no share-count
-- column exists on user_trade_events). Outcome key = COALESCE(outcome_code,
-- side) so binary AMM games, v2 order-book markets and multi-outcome futures
-- pools all match on the same key.
--
-- Consumers (all window on realized_at; ROI = SUM(return)/SUM(cost) - 1):
--   * masterMetrics.ts       — /api/leaderboard/users (leaderboard + profile)
--   * leagueChat.ts          — expert gate + author ROI badges
--   * socialTags.ts          — Sharp badge rebuild
--   * profilePortfolio.ts    — profile stats.roiNet
-- ============================================================================

CREATE OR REPLACE VIEW public.user_realized_events AS
WITH ev AS (
  SELECT
    id,
    LOWER(user_address)            AS u,
    game_id,
    league,
    type,
    COALESCE(outcome_code, side)   AS okey,
    "timestamp"                    AS ts,
    COALESCE(gross_in_dec,  0)::numeric AS gross_in,
    COALESCE(net_out_dec,   0)::numeric AS net_out,
    COALESCE(gross_out_dec, 0)::numeric AS gross_out,
    avg_price_bps
  FROM public.user_trade_events
  WHERE promo_redemption_id IS NULL
),
game_settle AS (
  -- Best available "when did this game settle" signal: the first CLAIM by any
  -- user (winners claim right after settlement).
  SELECT game_id, MIN("timestamp") AS first_claim_ts
  FROM public.user_trade_events
  WHERE type = 'CLAIM'
  GROUP BY game_id
),
buys AS (
  SELECT u, game_id, okey,
         SUM(gross_in)                                        AS buy_cost,
         SUM(gross_in * 10000.0 / NULLIF(avg_price_bps, 0))   AS buy_shares
  FROM ev
  WHERE type = 'BUY'
  GROUP BY 1, 2, 3
),
sells AS (
  SELECT u, game_id, okey, id, ts, net_out,
         gross_out * 10000.0 / NULLIF(avg_price_bps, 0) AS shares_sold,
         SUM(gross_out * 10000.0 / NULLIF(avg_price_bps, 0))
           OVER (PARTITION BY u, game_id, okey ORDER BY ts, id) AS cum_shares
  FROM ev
  WHERE type = 'SELL'
),
sell_events AS (
  SELECT s.u, s.game_id, s.okey, s.ts,
         s.net_out AS realized_return,
         CASE WHEN COALESCE(b.buy_shares, 0) > 0 THEN
           b.buy_cost / b.buy_shares *
           GREATEST(0, LEAST(s.cum_shares, b.buy_shares)
                       - LEAST(s.cum_shares - s.shares_sold, b.buy_shares))
         ELSE 0 END AS realized_cost
  FROM sells s
  LEFT JOIN buys b USING (u, game_id, okey)
),
sold_cost AS (
  SELECT u, game_id, okey, SUM(realized_cost) AS sold_cost
  FROM sell_events
  GROUP BY 1, 2, 3
),
rem AS (
  SELECT b.u, b.game_id, b.okey,
         GREATEST(b.buy_cost - COALESCE(sc.sold_cost, 0), 0) AS remaining_cost
  FROM buys b
  LEFT JOIN sold_cost sc USING (u, game_id, okey)
),
claims AS (
  SELECT u, game_id, MAX(ts) AS ts, SUM(net_out) AS claim_out
  FROM ev
  WHERE type = 'CLAIM'
  GROUP BY 1, 2
),
claim_events AS (
  SELECT c.u, c.game_id, c.ts,
         c.claim_out AS realized_return,
         CASE
           WHEN g.resolution_type IN ('NORMAL', 'RESOLVED')
                AND COALESCE(g.winner_team_code, g.winner_side) IS NOT NULL
                AND g.winner_side IS DISTINCT FROM 'TIE'
             THEN COALESCE((SELECT r.remaining_cost FROM rem r
                            WHERE r.u = c.u AND r.game_id = c.game_id
                              AND r.okey = COALESCE(g.winner_team_code, g.winner_side)), 0)
           ELSE COALESCE((SELECT SUM(r.remaining_cost) FROM rem r
                          WHERE r.u = c.u AND r.game_id = c.game_id), 0)
         END AS realized_cost
  FROM claims c
  JOIN public.games g ON g.game_id = c.game_id
),
loss_events AS (
  SELECT r.u, r.game_id,
         COALESCE(gs.first_claim_ts,
                  CASE WHEN g.lock_time < 32503680000 THEN g.lock_time
                       ELSE EXTRACT(EPOCH FROM g.updated_at)::bigint END) AS ts,
         0::numeric       AS realized_return,
         r.remaining_cost AS realized_cost
  FROM rem r
  JOIN public.games g ON g.game_id = r.game_id
  LEFT JOIN game_settle gs ON gs.game_id = r.game_id
  WHERE g.is_final = true
    AND g.resolution_type IN ('NORMAL', 'RESOLVED')
    AND COALESCE(g.winner_team_code, g.winner_side) IS NOT NULL
    AND g.winner_side IS DISTINCT FROM 'TIE'
    AND r.remaining_cost > 0.000001
    AND r.okey IS DISTINCT FROM COALESCE(g.winner_team_code, g.winner_side)
)
SELECT u AS user_address, g.league, x.game_id, x.kind,
       x.ts::bigint AS realized_at,
       x.realized_return, x.realized_cost
FROM (
  SELECT u, game_id, ts, realized_return, realized_cost, 'SELL'::text  AS kind FROM sell_events
  UNION ALL
  SELECT u, game_id, ts, realized_return, realized_cost, 'CLAIM'::text AS kind FROM claim_events
  UNION ALL
  SELECT u, game_id, ts, realized_return, realized_cost, 'LOSS'::text  AS kind FROM loss_events
) x
JOIN public.games g ON g.game_id = x.game_id;

-- Security: like the other P&L views this must not be readable by anon
-- (DEFINER-style views bypass table RLS — see the 2026-08 lockdown).
REVOKE ALL ON public.user_realized_events FROM anon;
REVOKE ALL ON public.user_realized_events FROM authenticated;
