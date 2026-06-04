-- src/db/migrations/2026-06-04_referral_mutual_trade.sql
--
-- Adds the two-sided "refer a friend, both trade $20, both get a $20 free bet"
-- promo on top of the existing System B free-bet framework. Additive only —
-- no existing column is altered, and the legacy code-redemption flow is
-- untouched.
--
-- Apply in Supabase SQL editor. Safe to re-run. NOTE: the campaign INSERT in
-- Step 2 (referral_campaign_seed.sql) MUST be run as a SEPARATE statement
-- AFTER this file commits, because Postgres will not let a freshly-added enum
-- value be used in the same transaction that added it.

-- ── 1. New unlock condition enum value ──────────────────────────────────────
-- 'mutual_referral_trade' = both the referrer and the referee must each
-- independently reach unlock_min_trade_usdc (held-to-settlement) before either
-- redemption flips to 'eligible'.
ALTER TYPE public.promotion_unlock_condition
  ADD VALUE IF NOT EXISTS 'mutual_referral_trade';

-- ── 2. promo_redemptions.referral_invite_id ─────────────────────────────────
-- Links the two redemptions of a referral pair back to the invites row that
-- produced them. invites.id is bigint, so this column is bigint (NOT uuid).
ALTER TABLE public.promo_redemptions
  ADD COLUMN IF NOT EXISTS referral_invite_id bigint;

-- FK to invites(id). Wrapped so re-runs don't error on the duplicate constraint.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'promo_redemptions_referral_invite_fk'
  ) THEN
    ALTER TABLE public.promo_redemptions
      ADD CONSTRAINT promo_redemptions_referral_invite_fk
      FOREIGN KEY (referral_invite_id)
      REFERENCES public.invites (id)
      ON DELETE SET NULL;
  END IF;
END $$;

-- ── 3. promo_redemptions.qualify_by ─────────────────────────────────────────
-- The 30-day mutual-trade deadline. If both sides haven't each traded
-- unlock_min_trade_usdc by this time, expirePromoRedemptions voids the
-- still-pending pair.
ALTER TABLE public.promo_redemptions
  ADD COLUMN IF NOT EXISTS qualify_by timestamptz;

-- ── 4. One redemption per (invite, beneficiary) ─────────────────────────────
-- Makes pair-creation idempotent: a given wallet can hold at most one
-- redemption for a given invite. Partial so it never touches non-referral
-- redemptions (which keep referral_invite_id NULL).
CREATE UNIQUE INDEX IF NOT EXISTS promo_redemptions_referral_pair_uniq
  ON public.promo_redemptions (referral_invite_id, lower(user_address))
  WHERE referral_invite_id IS NOT NULL;

-- ── 5. Lookup index for pair re-evaluation ──────────────────────────────────
CREATE INDEX IF NOT EXISTS promo_redemptions_referral_invite_idx
  ON public.promo_redemptions (referral_invite_id)
  WHERE referral_invite_id IS NOT NULL;
