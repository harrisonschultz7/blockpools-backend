-- Applied to Supabase 2026-10-16 (migration copy_stop_loss_intent).
-- A bot stop-out (limits.js tryStop) now emits a 'stop' copy signal; the fan-out
-- cancels the copied exit and market-sells the subscriber's holding.
ALTER TABLE bots.trade_intents DROP CONSTRAINT trade_intents_kind_check;
ALTER TABLE bots.trade_intents ADD CONSTRAINT trade_intents_kind_check CHECK (kind = ANY (ARRAY['enter','exit','reprice','stop']));
ALTER TABLE copy.orders DROP CONSTRAINT orders_kind_check;
ALTER TABLE copy.orders ADD CONSTRAINT orders_kind_check CHECK (kind = ANY (ARRAY['enter','exit','reprice','stop']));
