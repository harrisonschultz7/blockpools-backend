-- src/services/promotions/SCHEMA_ADDITIONS.sql
--
-- Final schema additions on top of the already-migrated promo tables. Three
-- changes, all idempotent:
--
--   1. promo_redemptions.outcome_index   (integer)
--   2. promo_redemptions.tx_hash         (text + lower() index)
--   3. user_trade_events.effective_user_address (generated column, replaces
--      the user_trade_events_attributed view)
--
-- Apply order: run top-to-bottom in Supabase SQL editor. Safe to re-run.

-- ── 1. promo_redemptions.outcome_index ──────────────────────────────────────
-- Settlement compares this against games.winning_outcome_index to decide win
-- vs loss without joining through trade events.

ALTER TABLE public.promo_redemptions
  ADD COLUMN IF NOT EXISTS outcome_index integer;

-- ── 2. promo_redemptions.tx_hash ────────────────────────────────────────────
-- Recorded by placeFreeBet at the moment the on-chain BUY confirms. The
-- persistTrades pre-insert hook uses (pool_address, tx_hash) to match a
-- funding-wallet trade back to the redemption it belongs to.

ALTER TABLE public.promo_redemptions
  ADD COLUMN IF NOT EXISTS tx_hash text;

CREATE INDEX IF NOT EXISTS promo_redemptions_tx_hash_idx
  ON public.promo_redemptions (lower(tx_hash))
  WHERE tx_hash IS NOT NULL;

-- ── 3. effective_user_address generated column on user_trade_events ─────────
-- Replaces the user_trade_events_attributed view so promo attribution lives
-- on the base table — no duplicate. Postgres maintains the column
-- automatically on insert/update.
--
-- Stats queries that want attribution change one identifier:
--     user_address  →  effective_user_address
-- Stats queries that want strictly on-chain stats keep using user_address.

DROP VIEW IF EXISTS public.user_trade_events_attributed;

ALTER TABLE public.user_trade_events
  ADD COLUMN IF NOT EXISTS effective_user_address text
  GENERATED ALWAYS AS (COALESCE(beneficiary_address, user_address)) STORED;

CREATE INDEX IF NOT EXISTS user_trade_events_effective_user_idx
  ON public.user_trade_events (lower(effective_user_address));
