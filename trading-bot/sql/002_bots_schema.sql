-- trading-bot/sql/002_bots_schema.sql
--
-- Bot registry, trade ledger and NAV history.
--
-- NAV IS THE PRODUCT. The homepage ROI and performance chart read
-- bots.nav_history, never a sum of realised trades. Realised-only math is what
-- produced the 1,098% ROI figures on the user leaderboard: closed winners are
-- counted while open losers are invisible, so the number only goes up.
-- nav_history marks open positions to the live book every day, so it can fall.

create schema if not exists bots;

-- == Bot registry ===========================================================
create table if not exists bots.bot (
  id           text primary key,          -- 'adam-7'
  name         text not null,             -- display name on the homepage
  league       text not null default 'NFL',
  risk_tier    text not null,             -- 'medium'
  mode         text not null default 'paper',   -- 'paper' | 'live'
  enabled      boolean not null default true,
  config       jsonb not null,            -- frozen copy of config.json at launch
  starting_nav numeric not null default 10000,
  created_at   timestamptz not null default now()
);

-- == Trade ledger ===========================================================
-- Every decision the bot commits to. In paper mode fill_price comes from
-- walking recorded book depth (see src/exec/paper.js), NOT from the midpoint --
-- a mid-fill ledger flatters the bot by exactly the spread it never paid.
create table if not exists bots.trades (
  id             bigserial primary key,
  bot_id         text not null references bots.bot (id),
  game_id        text not null,
  condition_id   text,
  token_id       text,
  side           text not null,        -- 'home' | 'away'  (the side bought)
  action         text not null,        -- 'BUY' | 'SELL'
  mode           text not null,        -- 'paper' | 'live'

  -- decision inputs, frozen at signal time
  p_market       numeric not null,
  p_fair         numeric not null,
  edge           numeric not null,     -- p_fair - price paid, in prob points
  conviction     numeric,              -- 0..1, drives size
  kelly_fraction numeric,
  feature_id     bigint references sports.features (id),

  -- execution
  intended_price numeric not null,
  fill_price     numeric,              -- depth-walked average, NOT the mid
  shares         numeric,
  notional_usd   numeric,
  slippage_bps   numeric,              -- fill_price vs best_ask at signal time
  fill_detail    jsonb,                -- levels consumed, for audit
  unfilled_shares numeric default 0,   -- depth ran out: the honest partial

  -- lifecycle
  opened_at      timestamptz not null default now(),
  closed_at      timestamptz,
  settled        boolean not null default false,
  won            boolean,
  pnl_usd        numeric,

  -- grading
  closing_price  numeric,              -- market price at kickoff
  clv_bps        numeric               -- (closing - fill) in bps; THE metric
);
create index if not exists bots_trades_bot_idx  on bots.trades (bot_id, opened_at desc);
create index if not exists bots_trades_game_idx on bots.trades (game_id);

-- == Open positions =========================================================
create table if not exists bots.positions (
  bot_id      text not null references bots.bot (id),
  game_id     text not null,
  token_id    text not null,
  side        text not null,
  shares      numeric not null default 0,
  avg_price   numeric,
  cost_usd    numeric,
  opened_at   timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  primary key (bot_id, token_id)
);

-- == NAV history -- what the homepage chart plots ===========================
-- One row per bot per day. position_value_usd marks OPEN positions to the last
-- recorded book mid, so an unrealised loss shows up the day it happens.
create table if not exists bots.nav_history (
  bot_id             text not null references bots.bot (id),
  d                  date not null,
  nav_usd            numeric not null,
  cash_usd           numeric not null,
  position_value_usd numeric not null default 0,
  realized_pnl_usd   numeric not null default 0,
  unrealized_pnl_usd numeric not null default 0,
  open_positions     int not null default 0,
  computed_at        timestamptz not null default now(),
  primary key (bot_id, d)
);

-- == Selectivity / bias audit ===============================================
-- The bot is meant to SKIP most games. This records every game it looked at and
-- why it passed, so "traded 3 of 16 games" is provable rather than asserted.
-- fav_side also feeds the favourite-vs-underdog bias check: if the bot only
-- scores on underdogs, that is a bias, not an edge.
create table if not exists bots.decisions (
  id            bigserial primary key,
  bot_id        text not null references bots.bot (id),
  game_id       text not null,
  asof_ts       timestamptz not null default now(),
  p_market      numeric,
  p_fair        numeric,
  edge          numeric,
  acted         boolean not null,
  skip_reason   text,        -- 'edge_below_threshold' | 'low_confidence' | ...
  bet_side      text,
  fav_side      text,        -- which side the market favoured
  bet_on_dog    boolean,
  trade_id      bigint references bots.trades (id)
);
create index if not exists bots_decisions_bot_idx on bots.decisions (bot_id, asof_ts desc);
