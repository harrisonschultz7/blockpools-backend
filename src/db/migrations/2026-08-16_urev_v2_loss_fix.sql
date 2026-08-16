-- ============================================================================
-- FIX: v2 losses never realized in user_realized_events.
--
-- Symptom (found 2026-08-16 via Goldiblocks 7d ROI = +77.4% when his history
-- said −9%): v2 order-book settlements write games.winning_outcome_index (0|1)
-- but NOT winner_side / winner_team_code, and recordV2Resolution only writes
-- auto-CLAIM rows for non-MM winners. On thin markets where the only winner is
-- the seed bot (an MM wallet), a resolved game ends up with:
--   * winner_team_code IS NULL AND winner_side IS NULL  → loss gate never fires
--   * zero CLAIM rows                                    → no settle timestamp
-- so every LOSING stake on such games stays invisible and short-window ROI
-- skews systematically positive (wins/sells realize, losses don't).
--
-- Fix: derive the winner key from winning_outcome_index → team_a/b_code for
-- plain game ids (group sub-markets `parent::CODE` are excluded — their okey
-- domain isn't the team codes — they keep the old behavior). Loss timestamp
-- falls back to games.updated_at, which recordV2Resolution bumps at settlement.
--
-- Idempotent; CREATE OR REPLACE keeps existing grants, REVOKEs re-run anyway.
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
gwin AS (
  -- Winner key per game, across both settlement pipelines:
  --   * v1 AMM sports scan → winner_team_code / winner_side ('TIE' folds to
  --     NULL so tie/void games never emit LOSS rows)
  --   * v2 order-book (recordV2Resolution) → winning_outcome_index mapped to
  --     the team code; plain game ids only (group sub-markets `parent::CODE`
  --     use a different okey domain)
  SELECT game_id,
         COALESCE(
           winner_team_code,
           NULLIF(winner_side, 'TIE'),
           CASE WHEN game_id NOT LIKE '%::%' THEN
             CASE winning_outcome_index
               WHEN 0 THEN team_a_code
               WHEN 1 THEN team_b_code
             END
           END
         ) AS wkey
  FROM public.games
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
                AND gw.wkey IS NOT NULL
             THEN COALESCE((SELECT r.remaining_cost FROM rem r
                            WHERE r.u = c.u AND r.game_id = c.game_id
                              AND r.okey = gw.wkey), 0)
           ELSE COALESCE((SELECT SUM(r.remaining_cost) FROM rem r
                          WHERE r.u = c.u AND r.game_id = c.game_id), 0)
         END AS realized_cost
  FROM claims c
  JOIN public.games g ON g.game_id = c.game_id
  LEFT JOIN gwin gw ON gw.game_id = c.game_id
),
loss_events AS (
  SELECT r.u, r.game_id,
         COALESCE(gs.first_claim_ts,
                  CASE WHEN g.lock_time < 32503680000 THEN g.lock_time
                       ELSE EXTRACT(EPOCH FROM g.updated_at)::bigint END,
                  EXTRACT(EPOCH FROM g.updated_at)::bigint) AS ts,
         0::numeric       AS realized_return,
         r.remaining_cost AS realized_cost
  FROM rem r
  JOIN public.games g ON g.game_id = r.game_id
  LEFT JOIN gwin gw ON gw.game_id = r.game_id
  LEFT JOIN game_settle gs ON gs.game_id = r.game_id
  WHERE g.is_final = true
    AND g.resolution_type IN ('NORMAL', 'RESOLVED')
    AND gw.wkey IS NOT NULL
    AND r.remaining_cost > 0.000001
    AND r.okey IS DISTINCT FROM gw.wkey
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

REVOKE ALL ON public.user_realized_events FROM anon;
REVOKE ALL ON public.user_realized_events FROM authenticated;
