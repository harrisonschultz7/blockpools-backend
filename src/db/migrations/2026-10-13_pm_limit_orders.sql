-- src/db/migrations/2026-10-13_pm_limit_orders.sql
--
-- Resting Polymarket limit orders placed through BlockPools, per user.
--
-- The venue will not report these orders back: listOpenOrders returns nothing
-- for Deposit-Wallet orders and fetchOrder's response fails the SDK schema. So
-- the app records each resting limit at placement, and this table makes that
-- record the same on every device (it lived in one browser's localStorage).
-- Keyed by the Privy user id, which is identical across devices.
--
-- Written only by the backend (pg pool); RLS on and anon/authenticated revoked.

CREATE TABLE IF NOT EXISTS public.pm_limit_orders (
  id             text PRIMARY KEY,            -- venue order id (0x…)
  user_id        text NOT NULL,               -- Privy DID
  owner_address  text,                        -- app identity (smart wallet), for joins
  market_id      text NOT NULL,               -- Polymarket conditionId, lowercase
  outcome_index  smallint NOT NULL CHECK (outcome_index IN (0, 1)),
  side           text NOT NULL CHECK (side IN ('buy', 'sell')),
  price          numeric NOT NULL CHECK (price > 0 AND price < 1),
  shares         numeric NOT NULL CHECK (shares > 0),
  status         text NOT NULL DEFAULT 'live' CHECK (status IN ('live', 'closed')),
  closed_reason  text,                        -- cancelled | filled | edited | expired | venue_closed
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  closed_at      timestamptz
);

CREATE INDEX IF NOT EXISTS pm_limit_orders_user_live_idx
  ON public.pm_limit_orders (user_id, created_at DESC) WHERE status = 'live';

ALTER TABLE public.pm_limit_orders ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.pm_limit_orders FROM anon, authenticated;
