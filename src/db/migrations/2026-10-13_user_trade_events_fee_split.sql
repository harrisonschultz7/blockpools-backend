-- Applied to Supabase 2026-10-13 (migration user_trade_events_fee_split_polygon_username).
-- Fee split + bettor identity on every trade row, stamped by a BEFORE trigger so
-- every write path (record-trade, record-claim, pmSettlementWorker, subgraph
-- upsert) gets it with no app change. user_address is left as the app identity
-- (smart wallet) because History / leaderboard / ROI key on it.
ALTER TABLE public.user_trade_events
  ADD COLUMN IF NOT EXISTS polymarket_fee_dec numeric,
  ADD COLUMN IF NOT EXISTS blockpools_fee_dec numeric,
  ADD COLUMN IF NOT EXISTS total_fee_dec numeric,
  ADD COLUMN IF NOT EXISTS polygon_address text,
  ADD COLUMN IF NOT EXISTS username text;

CREATE INDEX IF NOT EXISTS users_trading_address_idx ON public.users (lower(trading_address));

CREATE OR REPLACE FUNCTION public.user_trade_events_stamp()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  u record;
  is_pm boolean := NEW.game_id ILIKE '%-pm-%' OR NEW.id ILIKE '%-direct-pm-%';
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

  -- Polymarket rows: fee_dec is the venue (sports) fee; BlockPools builder fee
  -- is 1% taker on notional (BUY net stake, SELL gross proceeds).
  IF is_pm THEN
    NEW.polymarket_fee_dec := COALESCE(NEW.fee_dec, 0);
    NEW.blockpools_fee_dec := CASE NEW.type
      WHEN 'BUY'  THEN round(0.01 * COALESCE(NEW.net_stake_dec, 0), 6)
      WHEN 'SELL' THEN round(0.01 * COALESCE(NEW.gross_out_dec, 0), 6)
      ELSE 0 END;
    NEW.total_fee_dec := NEW.polymarket_fee_dec + NEW.blockpools_fee_dec;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS user_trade_events_stamp_trg ON public.user_trade_events;
CREATE TRIGGER user_trade_events_stamp_trg
  BEFORE INSERT OR UPDATE ON public.user_trade_events
  FOR EACH ROW EXECUTE FUNCTION public.user_trade_events_stamp();

-- Optional backfill of existing rows (trigger fills them on a no-op update):
-- UPDATE public.user_trade_events SET user_address = user_address WHERE username IS NULL;
