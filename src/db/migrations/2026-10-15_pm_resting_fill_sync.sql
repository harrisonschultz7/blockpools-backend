-- src/db/migrations/2026-10-15_pm_resting_fill_sync.sql
-- Applied to Supabase 2026-10-15 (migration pm_resting_fill_sync).
--
-- Resting limit fills -> user_trade_events (src/workers/pmFillSyncWorker.ts).
--   game_id / meta   the app's game id + display fields, sent when the order is
--                    recorded, so a later fill is written under the same game
--   filled_shares    what the fill sync has attributed to the order so far
ALTER TABLE public.pm_limit_orders
  ADD COLUMN IF NOT EXISTS game_id       text,
  ADD COLUMN IF NOT EXISTS meta          jsonb,
  ADD COLUMN IF NOT EXISTS filled_shares numeric NOT NULL DEFAULT 0;

-- Fee stamping: maker fills ('pmfill-m-…') pay no BlockPools builder fee (0%
-- maker), and Polymarket's sports fee is taker-only, so their fee_dec is 0.
CREATE OR REPLACE FUNCTION public.user_trade_events_stamp()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  u record;
  is_pm boolean := NEW.game_id ILIKE '%-pm-%' OR NEW.id ILIKE '%-direct-pm-%' OR NEW.id LIKE 'pmfill-%';
BEGIN
  IF NEW.username IS NULL OR NEW.polygon_address IS NULL THEN
    SELECT username, trading_address INTO u
      FROM public.users
     WHERE lower(primary_address) = lower(COALESCE(NEW.beneficiary_address, NEW.user_address))
        OR lower(trading_address) = lower(COALESCE(NEW.beneficiary_address, NEW.user_address))
     LIMIT 1;
    IF FOUND THEN
      NEW.username := COALESCE(NEW.username, u.username);
      NEW.polygon_address := COALESCE(NEW.polygon_address, lower(u.trading_address));
    END IF;
  END IF;

  IF is_pm THEN
    NEW.polymarket_fee_dec := COALESCE(NEW.fee_dec, 0);
    NEW.blockpools_fee_dec := CASE
      WHEN NEW.id LIKE 'pmfill-m-%' THEN 0
      WHEN NEW.type = 'BUY'  THEN round(0.01 * COALESCE(NEW.net_stake_dec, 0), 6)
      WHEN NEW.type = 'SELL' THEN round(0.01 * COALESCE(NEW.gross_out_dec, 0), 6)
      ELSE 0 END;
    NEW.total_fee_dec := NEW.polymarket_fee_dec + NEW.blockpools_fee_dec;
  END IF;
  RETURN NEW;
END $$;
