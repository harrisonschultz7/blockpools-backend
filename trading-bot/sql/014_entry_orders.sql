-- trading-bot/sql/014_entry_orders.sql
--
-- MAKER ENTRIES (user, 2026-11-25). Instead of taking the ask when the model
-- decides to trade, the bot rests a BID just under the mid and lets the market
-- come to it; if nothing has filled by policy.makerEntry.fallbackMinutesBeforeTip
-- it cancels and takes the ask as before. Replay (2025-26 on, 662 Axis picks):
-- +1.1 points of return per trade, fills won MORE often than their price.
--
-- Its own table, not bots.limit_orders: every reader of limit_orders (exits,
-- reprices, held-stop sweeps, the copy fan-out, the ops monitor) assumes a row
-- there is a SELL against an existing trade. A resting BUY has no trade yet.
create table if not exists bots.entry_orders (
  id            bigserial primary key,
  bot_id        text not null references bots.bot (id),
  game_id       text not null,
  condition_id  text,
  token_id      text not null,
  side          text not null,                  -- home | away
  limit_price   numeric not null,
  shares        numeric not null,
  notional_usd  numeric not null,
  -- what the model said when it posted, so a fill can be booked like any trade
  p_market      numeric,
  p_fair        numeric,
  edge          numeric,                         -- vs the ask at posting time
  feature_id    bigint,
  nav_usd       numeric,
  decision      jsonb,
  status        text not null default 'open',   -- open | filled | cancelled
  closed_reason text,                            -- fallback_taker | edge_gone | side_flip | game_started
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  filled_at     timestamptz,
  fill_detail   jsonb,
  trade_id      bigint references bots.trades (id)
);
create unique index if not exists bots_entry_orders_one_open
  on bots.entry_orders (bot_id, game_id) where status = 'open';
create index if not exists bots_entry_orders_bot_status on bots.entry_orders (bot_id, status);
revoke all on bots.entry_orders from anon, authenticated;
