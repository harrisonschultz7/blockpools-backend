-- trading-bot/sql/006_trade_intents.sql
--
-- THE COPY-TRADING SIGNAL. An append-only log of what a bot decided, in a form
-- another account can act on.
--
-- Why this exists separately from bots.trades: a trade row records what the bot
-- DID, in the bot's own dollars, after its own fill. A subscriber needs what the
-- bot DECIDED, in a size-agnostic form, as early as possible. nav_fraction is
-- the whole point -- the bot's position as a fraction of its own NAV, so a
-- subscriber with a $500 sleeve and a bot with a $10,000 portfolio arrive at the
-- same percentage exposure without either knowing the other's size.
--
-- Three kinds:
--   enter   -- the bot bought. nav_fraction is set.
--   exit    -- the bot posted a resting sell. limit_price is the target.
--   reprice -- the bot moved that resting sell before kickoff.
--
-- APPEND ONLY, and deliberately WITHOUT a consumed_at column. One intent fans
-- out to many subscribers, each of which can succeed, partially fill, or be
-- rejected for its own reasons, so "was this processed" is a fact about a
-- (subscription, intent) pair rather than about the intent. That state belongs
-- in the copy schema; this table stays a clean record of what the model said.
--
-- No NOTIFY trigger on purpose. This database is reached through a transaction
-- pooler -- two clients checked out of the pool report the same pg_backend_pid,
-- which is also why session-scoped advisory locks silently fail to isolate here.
-- LISTEN needs a durable session and cannot be relied on through that pooler, so
-- the fan-out worker polls `where id > last_seen` instead. At a one-second poll
-- that is cheaper than it sounds and it cannot silently stop working.
--
-- Useful on its own before any of that ships: an auditable record of every
-- decision the bot made, separate from whether its own paper fill succeeded.
create table if not exists bots.trade_intents (
  id           bigserial primary key,
  bot_id       text not null references bots.bot (id),
  kind         text not null check (kind in ('enter', 'exit', 'reprice')),
  asof_ts      timestamptz not null default now(),

  -- what to trade
  game_id      text,
  condition_id text,
  token_id     text not null,
  side         text not null,
  market_type  text not null default 'moneyline',
  line         numeric,

  -- how
  limit_price  numeric not null,
  -- enter only. Measured on the FILLED cost, not the intended notional: a
  -- subscriber should mirror the exposure the bot actually took, and a partial
  -- fill means those differ.
  nav_fraction numeric,
  nav_usd      numeric,

  -- links an exit or reprice back to the entry it manages
  bot_trade_id bigint references bots.trades (id) on delete cascade
);

-- The only read pattern a fan-out worker has: everything newer than the last id
-- it handled, oldest first.
create index if not exists bots_trade_intents_idx
  on bots.trade_intents (id);

-- And the audit read: one bot's decisions on one game.
create index if not exists bots_trade_intents_game_idx
  on bots.trade_intents (bot_id, game_id, asof_ts desc);

revoke all on bots.trade_intents from anon, authenticated;
revoke all on sequence bots.trade_intents_id_seq from anon, authenticated;
