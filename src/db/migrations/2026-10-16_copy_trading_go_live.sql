-- Applied to Supabase 2026-10-16 (migration copy_trading_go_live).
-- copy.positions: closed_at / realized_usd -- a finished position (exited or
--   settled) is closed and its P&L banked into copy.subscriptions.realized_pnl
--   by the fan-out (reconcileSleeves), so sleeve NAV follows results.
-- copy.consents: country / region -- the subscriber's Polymarket geoblock answer
--   at subscribe time (blocked regions are refused by /api/copy/subscribe).
ALTER TABLE copy.positions
  ADD COLUMN IF NOT EXISTS closed_at    timestamptz,
  ADD COLUMN IF NOT EXISTS realized_usd numeric;
ALTER TABLE copy.consents
  ADD COLUMN IF NOT EXISTS country text,
  ADD COLUMN IF NOT EXISTS region  text;
