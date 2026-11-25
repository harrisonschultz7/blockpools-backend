-- trading-bot/sql/015_copy_maker_bids.sql
--
-- COPIERS REST THE SAME BID AS THE BOT (user, 2026-11-25). With makerEntry (014)
-- the bot rests a bid under the mid; copiers used to wait for the bot's PAPER
-- fill and then buy at market, paying Polymarket's taker fee (5% x (1 - price))
-- and the 1% builder fee. Now each copier rests their own post-only bid at the
-- bot's price (maker: 0% + 0%), and at the bot's fallback time whatever has not
-- filled is cancelled and topped up at market.
--
-- New intent kinds, all carrying entry_order_id (the bot's bots.entry_orders row):
--   bid          the bot posted a bid: copiers post theirs
--   bid_cancel   the bot withdrew it (edge gone, side flip, game started):
--                copiers cancel, keep whatever already filled, no top-up
--   bid_close    fallback time on a bid the BOT filled as a maker: copiers
--                cancel what is left and buy the rest at market
--   enter_maker  the bot's own maker fill: copiers do nothing yet (their bid is
--                still resting; bid_close tops them up)
-- An 'enter' that carries entry_order_id is the bot's fallback TAKER buy after
-- its bid did not fill: copiers close their bid and top up, like bid_close.
alter table bots.trade_intents add column if not exists entry_order_id bigint;
alter table bots.trade_intents drop constraint if exists trade_intents_kind_check;
alter table bots.trade_intents add constraint trade_intents_kind_check
  check (kind in ('enter', 'exit', 'reprice', 'stop', 'bid', 'bid_cancel', 'bid_close', 'enter_maker'));
alter table copy.orders drop constraint if exists orders_kind_check;
alter table copy.orders add constraint orders_kind_check
  check (kind in ('enter', 'exit', 'reprice', 'stop', 'bid', 'bid_cancel', 'bid_close', 'enter_maker'));
create index if not exists trade_intents_entry_order_idx on bots.trade_intents (entry_order_id) where entry_order_id is not null;
-- when copiers were told to close/top up this bid (once per bid)
alter table bots.entry_orders add column if not exists copy_closed_at timestamptz;
