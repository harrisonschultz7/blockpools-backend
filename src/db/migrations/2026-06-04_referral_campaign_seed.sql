-- src/db/migrations/2026-06-04_referral_campaign_seed.sql
--
-- Step 2 of the referral promo. Run this AFTER 2026-06-04_referral_mutual_trade.sql
-- has committed (the enum value 'mutual_referral_trade' must already exist).
--
-- Creates the single campaign row that every referral redemption attaches to.
-- There is no user-entered code — redemptions are created automatically by
-- createReferralRedemptions when an invited friend joins. `code` is kept unique
-- for internal lookup only.
--
-- EDIT BEFORE RUNNING:
--   - funding_wallet_address  → must equal PROMO_FUNDING_WALLET_ADDRESS in env
--   - eligible_leagues        → the leagues the $20 free bet may be placed on
--   - starts_at / expires_at  → the campaign window
--   - active                  → leave false until you're ready to go live
--
-- Re-running is a no-op (guarded by NOT EXISTS on the code).

INSERT INTO public.promotions
  (code, name, description, type, credit_usdc, unlock_condition,
   unlock_min_trade_usdc, is_repeatable,
   max_claims_total, max_claims_per_user, total_claimed,
   placement_window_hours, eligible_leagues, funding_wallet_address,
   active, starts_at)
SELECT
   'REFERRAL_MUTUAL_20',
   'Refer a friend — both get $20',
   'You and your friend each get a $20 risk-free bet once you have each traded $20.',
   'referral',
   20,
   'mutual_referral_trade',
   20,
   true,            -- referrer can earn one redemption per qualifying friend
   NULL,            -- no global cap (per-referrer cap of 10 enforced in code)
   10,              -- advisory; the hard per-referrer cap lives in the service
   0,
   168,             -- 7-day window to place the free bet once eligible
   ARRAY['NBA'],    -- EDIT: leagues the free bet is valid on
   '0x0000000000000000000000000000000000000000', -- EDIT: PROMO_FUNDING_WALLET_ADDRESS
   false,           -- EDIT: flip to true to launch
   now()
WHERE NOT EXISTS (
  SELECT 1 FROM public.promotions WHERE upper(code) = 'REFERRAL_MUTUAL_20'
);
