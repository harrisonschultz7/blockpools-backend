-- ============================================================================
-- Per-game accounting: capture promo (free-bet) activity on each sweep row and
-- expose a booking-ready P&L view.
--
-- Run this in the Supabase SQL editor. It is idempotent (safe to re-run).
--
-- Why: the sweeps table records the on-chain settlement but is blind to the
-- promo system. The promo hot wallet is just another bettor in the pool, so:
--   * a WINNING free bet depresses amount_swept (its winning shares are part of
--     `liability`, claimed back by the promo wallet) — the game LOOKS like a
--     pool/LP loss when the house only paid the user the *profit* (a marketing
--     cost) and recouped the credit.
--   * a LOSING free bet inflates amount_swept (the staked credit sits in the
--     pool, then is swept to treasury).
-- promo_redemptions already tracks the clean economics per pool_address
-- (= sweeps.contract_address). This migration joins the two.
--
-- UNITS: sweeps on-chain columns are in USDC base units (1e6: amount_swept
-- 100000000 = $100). promo_redemptions columns are in whole USDC dollars
-- (credit_usdc = 20). The new snapshot columns below store DOLLARS (matching
-- their source). The game_accounting view normalizes everything to dollars.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- (a) Promo snapshot columns on sweeps (dollars). Written at sweep time by the
--     POST /api/admin/sweeps handler (adminSweeps.ts). Nullable: rows with no
--     free bets simply stay null.
-- ----------------------------------------------------------------------------
ALTER TABLE public.sweeps
  ADD COLUMN IF NOT EXISTS promo_bets_count            integer,
  ADD COLUMN IF NOT EXISTS promo_credit_staked_usdc    numeric,
  ADD COLUMN IF NOT EXISTS promo_credit_won_usdc       numeric,
  ADD COLUMN IF NOT EXISTS promo_credit_lost_usdc      numeric,
  ADD COLUMN IF NOT EXISTS promo_payout_to_users_usdc  numeric,
  ADD COLUMN IF NOT EXISTS promo_credit_recovered_usdc numeric,
  ADD COLUMN IF NOT EXISTS promo_unsettled_count       integer,
  ADD COLUMN IF NOT EXISTS promo_snapshot_at           timestamptz,
  -- Gas valued in USD at sweep time (ETH/USD from Chainlink, posted by the
  -- sweeper). Populated going forward only — historical rows stay null because
  -- there's no captured sweep-time ETH price (gas is sub-cent on Arbitrum, so
  -- the impact of nulls on net P&L is negligible).
  ADD COLUMN IF NOT EXISTS gas_cost_usd                numeric,
  ADD COLUMN IF NOT EXISTS eth_usd_price               numeric;

-- ----------------------------------------------------------------------------
-- (b) One-time backfill of the snapshot columns from promo_redemptions, so all
--     historical sweeps rows are populated immediately. The live view in (c)
--     does not depend on these — it recomputes from promo_redemptions — but the
--     snapshot is handy for audit ("what did we know at sweep time").
-- ----------------------------------------------------------------------------
WITH promo AS (
  SELECT
    lower(pool_address) AS pool_address,
    count(*)                          FILTER (WHERE status IN ('placed','settled_win','settled_loss')) AS bets_count,
    coalesce(sum(credit_usdc)         FILTER (WHERE status IN ('placed','settled_win','settled_loss')), 0) AS credit_staked,
    coalesce(sum(credit_usdc)         FILTER (WHERE status = 'settled_win'),  0) AS credit_won,
    coalesce(sum(credit_usdc)         FILTER (WHERE status = 'settled_loss'), 0) AS credit_lost,
    coalesce(sum(payout_amount_usdc), 0) AS payout_to_users,
    coalesce(sum(treasury_recovered_usdc), 0) AS credit_recovered,
    count(*)                          FILTER (WHERE status = 'placed') AS unsettled_count
  FROM public.promo_redemptions
  WHERE pool_address IS NOT NULL
  GROUP BY lower(pool_address)
)
UPDATE public.sweeps s
   SET promo_bets_count            = p.bets_count,
       promo_credit_staked_usdc    = p.credit_staked,
       promo_credit_won_usdc       = p.credit_won,
       promo_credit_lost_usdc      = p.credit_lost,
       promo_payout_to_users_usdc  = p.payout_to_users,
       promo_credit_recovered_usdc = p.credit_recovered,
       promo_unsettled_count       = p.unsettled_count,
       promo_snapshot_at           = now()
  FROM promo p
 WHERE lower(s.contract_address) = p.pool_address;

-- ----------------------------------------------------------------------------
-- (c) Booking-ready per-game accounting view. Joins each sweep to a LIVE
--     aggregate of promo_redemptions (not the snapshot columns) so the numbers
--     are always current even if a free bet settled after the sweep was posted.
--     Everything is normalized to USDC dollars.
--
--     Accounting model (treasury + LP + promo wallet = one "house"; LP is
--     owner-funded only, so no external LP split):
--       organic_house_revenue = swept + promo_payout - promo_credit_lost
--         (strip promo distortion: add back winning-bet profit that depressed
--          the sweep; remove losing free-bet stake that inflated it)
--       net_game_pnl          = swept - lp_funded - promo_credit_lost
--         (true bottom line, cash basis, EXCLUDING gas)
--                              = organic_house_revenue - promo_payout - lp_funded
--
--     Caveats:
--       * fees_usdc is MEMO-ONLY — it is already inside amount_swept (the
--         contract's excess bundles fees + LP + losing stakes). Do NOT add it on
--         top of swept.
--       * gas_cost_native is native ETH (wei). Folding it into a USDC P&L needs
--         an ETH/USD price, which is left to a follow-up.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.game_accounting AS
WITH promo AS (
  SELECT
    lower(pool_address) AS pool_address,
    count(*)                          FILTER (WHERE status IN ('placed','settled_win','settled_loss')) AS bets_count,
    coalesce(sum(credit_usdc)         FILTER (WHERE status IN ('placed','settled_win','settled_loss')), 0) AS credit_staked,
    coalesce(sum(credit_usdc)         FILTER (WHERE status = 'settled_win'),  0) AS credit_won,
    coalesce(sum(credit_usdc)         FILTER (WHERE status = 'settled_loss'), 0) AS credit_lost,
    coalesce(sum(payout_amount_usdc), 0) AS payout_to_users,
    coalesce(sum(treasury_recovered_usdc), 0) AS credit_recovered,
    count(*)                          FILTER (WHERE status = 'placed') AS unsettled_count
  FROM public.promo_redemptions
  WHERE pool_address IS NOT NULL
  GROUP BY lower(pool_address)
)
SELECT
  -- identity
  s.id,
  s.chain_id,
  s.contract_address,
  s.game_id,
  s.league,
  s.team_a_code,
  s.team_b_code,
  s.winning_team,
  s.tx_hash,
  s.locked_at,
  s.swept_at,

  -- normalized on-chain figures (USDC dollars)
  (s.amount_swept            / 1e6) AS gross_swept_usdc,
  (s.total_fees_1pct         / 1e6) AS fees_usdc,            -- MEMO: already inside swept
  (s.lp_funded_total         / 1e6) AS lp_funded_usdc,
  (s.withdraw_fees_total     / 1e6) AS withdraw_fees_usdc,
  (s.withdraw_net_payout_total / 1e6) AS withdraw_net_payout_usdc,
  s.gas_cost_native,                                         -- native ETH (wei)
  s.gas_cost_usd,                                            -- gas valued in USD at sweep time (null on historical rows)
  s.eth_usd_price,                                           -- ETH/USD used for the conversion

  -- promo activity (USDC dollars), recomputed live from promo_redemptions
  coalesce(p.bets_count, 0)        AS promo_bets_count,
  coalesce(p.unsettled_count, 0)   AS promo_unsettled_count,
  coalesce(p.credit_staked, 0)     AS promo_funding_usdc,        -- free-bet stake deployed
  coalesce(p.credit_won, 0)        AS promo_credit_won_usdc,
  coalesce(p.credit_lost, 0)       AS promo_credit_lost_usdc,
  coalesce(p.payout_to_users, 0)   AS promo_payout_to_users_usdc, -- marketing expense
  coalesce(p.credit_recovered, 0)  AS promo_credit_recovered_usdc,

  -- derived accounting
  ((s.amount_swept / 1e6) + coalesce(p.payout_to_users, 0) - coalesce(p.credit_lost, 0))
    AS organic_house_revenue_usdc,
  ((s.amount_swept / 1e6) - (s.lp_funded_total / 1e6) - coalesce(p.credit_lost, 0))
    AS net_game_pnl_usdc,
  -- Same bottom line, less gas (valued in USD). gas_cost_usd is null on
  -- historical rows, so coalesce to 0 there (gas is sub-cent on Arbitrum).
  ((s.amount_swept / 1e6) - (s.lp_funded_total / 1e6) - coalesce(p.credit_lost, 0)
    - coalesce(s.gas_cost_usd, 0))
    AS net_game_pnl_incl_gas_usd
FROM public.sweeps s
LEFT JOIN promo p
  ON p.pool_address = lower(s.contract_address);
