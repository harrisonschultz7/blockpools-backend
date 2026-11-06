-- One-off, run once, by hand. Corrects pm ledger data against Polymarket's own
-- resolved markets (checked 2026-11-06). Every row touched belongs to one account,
-- 0x39ef251f74586c244829e1b19ecd344c2e4452ce. No funds move: this is our ledger.
--
-- Deploy the settlement worker fix first, so nothing re-settles wrongly afterwards.

BEGIN;

-- 1. Seven moneylines the worker settled off a sibling prop event's market.
UPDATE public.games g SET
  winning_outcome_index = v.idx, winner_side = v.side, winner_team_code = v.code, updated_at = now()
FROM (VALUES
  ('nfl-jax-bal-pm-1793927700', 0, 'A', 'JAX'),  -- 33-10, was BAL
  ('nfl-ne-mia-pm-1793568300',  0, 'A', 'NE'),   -- 24-19, was MIA
  ('nfl-dal-phi-pm-1793060100', 0, 'A', 'DAL'),  -- 38-21, was PHI
  ('nfl-nyg-hou-pm-1792947600', 0, 'A', 'NYG'),  -- 27-21, was HOU
  ('nfl-was-sf-pm-1792455300',  1, 'B', 'SF'),   -- 17-23, was WAS
  ('nfl-buf-lv-pm-1792355100',  0, 'A', 'BUF'),  -- 33-30, was LV
  ('nfl-ten-ind-pm-1792342800', 0, 'A', 'TEN')   -- 26-20, was IND
) AS v(game_id, idx, side, code)
WHERE g.game_id = v.game_id;

-- 2. JAX is the one wrong game held to the end, so it is owed its payout record:
--    the worker's own formula, shares = net stake / price, x $1. About $23.21.
INSERT INTO public.user_trade_events (
  id, user_address, beneficiary_address, game_id, league,
  type, side, outcome_index, outcome_code, timestamp, tx_hash,
  spot_price_bps, avg_price_bps, gross_in_dec, gross_out_dec, fee_dec,
  net_stake_dec, net_out_dec, cost_basis_closed_dec, realized_pnl_dec,
  inserted_at, updated_at
)
SELECT
  'claim-direct-pm-settle-' || b.game_id || '-' || lower(b.user_address),
  lower(b.user_address), NULL, b.game_id, b.league,
  'CLAIM', NULL, 0, 'JAX', extract(epoch FROM now())::bigint,
  'pm-settle-' || b.game_id || '-' || lower(b.user_address),
  NULL, NULL, 0, b.net_stake_dec / (b.avg_price_bps / 10000.0), 0,
  0, b.net_stake_dec / (b.avg_price_bps / 10000.0), 0, 0,
  now(), now()
FROM public.user_trade_events b
WHERE b.game_id = 'nfl-jax-bal-pm-1793927700' AND b.type = 'BUY' AND b.outcome_code = 'JAX'
ON CONFLICT (id) DO NOTHING;

-- 3. Three totals queued on Nov 1, before the Queue logged totals as their own
--    market (FE 44ed961). They sit on the moneyline's game id, which is why those
--    games read "O/U" and a sale shows as a separate team pick.
INSERT INTO public.games (
  game_id, league, lock_time, is_final, winning_outcome_index, winner_side, winner_team_code,
  resolution_type, team_a_code, team_b_code, team_a_name, team_b_name, market_type, market_question
) VALUES
  ('nfl-lv-nyj-pm-1793556000::ou-44.5', 'NFL', 1793556000, true, 0, 'A', 'OVER',  'RESOLVED', 'OVER', 'UNDER', 'Over 44.5', 'Under 44.5', 'PROP', 'Raiders vs. Jets: O/U 44.5'),
  ('nfl-min-det-pm-1793556000::ou-46.5', 'NFL', 1793556000, true, 1, 'B', 'UNDER', 'RESOLVED', 'OVER', 'UNDER', 'Over 46.5', 'Under 46.5', 'PROP', 'Vikings vs. Lions: O/U 46.5'),
  ('nfl-ne-mia-pm-1793568300::ou-42.5', 'NFL', 1793568300, true, 0, 'A', 'OVER',  'RESOLVED', 'OVER', 'UNDER', 'Over 42.5', 'Under 42.5', 'PROP', 'Patriots vs. Dolphins: O/U 42.5')
ON CONFLICT (game_id) DO NOTHING;

UPDATE public.user_trade_events SET game_id = 'nfl-lv-nyj-pm-1793556000::ou-44.5', updated_at = now()
 WHERE game_id = 'nfl-lv-nyj-pm-1793556000' AND type = 'BUY' AND outcome_code = 'UNDER';

UPDATE public.user_trade_events SET game_id = 'nfl-min-det-pm-1793556000::ou-46.5', outcome_code = 'UNDER', updated_at = now()
 WHERE game_id = 'nfl-min-det-pm-1793556000'
   AND ((type = 'BUY' AND outcome_code = 'UNDER') OR (type = 'SELL' AND outcome_code = 'DET'));

UPDATE public.user_trade_events SET game_id = 'nfl-ne-mia-pm-1793568300::ou-42.5', outcome_code = 'OVER', updated_at = now()
 WHERE game_id = 'nfl-ne-mia-pm-1793568300'
   AND ((type = 'BUY' AND outcome_code = 'OVER') OR (type = 'SELL' AND outcome_code = 'NE'));

-- Those three game rows go back to being plain moneylines.
UPDATE public.games SET market_type = 'GAME', market_question = NULL, updated_at = now()
 WHERE game_id IN ('nfl-lv-nyj-pm-1793556000', 'nfl-min-det-pm-1793556000', 'nfl-ne-mia-pm-1793568300');

-- Check before committing: expect 1 JAX claim near 23.21, and 1 / 3 / 3 events
-- on the three new totals. If anything looks off: ROLLBACK.
SELECT game_id, type, outcome_code, count(*) AS n,
       round(sum(gross_in_dec)::numeric, 2) AS gross_in, round(sum(gross_out_dec)::numeric, 2) AS gross_out
  FROM public.user_trade_events
 WHERE game_id IN ('nfl-jax-bal-pm-1793927700', 'nfl-lv-nyj-pm-1793556000::ou-44.5',
                   'nfl-min-det-pm-1793556000::ou-46.5', 'nfl-ne-mia-pm-1793568300::ou-42.5')
 GROUP BY 1, 2, 3 ORDER BY 1, 2;

COMMIT;
