-- src/db/migrations/2026-10-14_pm_limit_orders_carry_over.sql
-- Applied to Supabase 2026-10-14 (migration pm_limit_orders_carry_over).
--
-- KEEP AFTER KICKOFF. Polymarket clears every resting order on a sports market
-- at the official game start. A user who opts in (and has granted our Privy
-- session signer) has the order re-placed for them shortly after kickoff by
-- trading-bot/run/limit-carryover.js.
--
--   token_id        CLOB outcome token the order is on (needed to re-place)
--   signer_address  the user's embedded EOA that signs the order
--   game_start_at   official start; the order is cleared at this moment
--   carry_over      user opted in AND delegation verified at record time
--   carry_status    pending -> placing -> placed | failed | no_permission | no_shares
--   carry_attempts / carry_error / carry_next_at   retry bookkeeping
--   carried_from    on the re-placed row: the original (pre-kickoff) order id
ALTER TABLE public.pm_limit_orders
  ADD COLUMN IF NOT EXISTS token_id        text,
  ADD COLUMN IF NOT EXISTS signer_address  text,
  ADD COLUMN IF NOT EXISTS game_start_at   timestamptz,
  ADD COLUMN IF NOT EXISTS carry_over      boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS carry_status    text,
  ADD COLUMN IF NOT EXISTS carry_attempts  integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS carry_error     text,
  ADD COLUMN IF NOT EXISTS carry_next_at   timestamptz,
  ADD COLUMN IF NOT EXISTS carried_from    text;

CREATE INDEX IF NOT EXISTS pm_limit_orders_carry_due_idx
  ON public.pm_limit_orders (game_start_at)
  WHERE status = 'live' AND carry_over AND carry_status = 'pending';
