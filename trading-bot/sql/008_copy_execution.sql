-- trading-bot/sql/008_copy_execution.sql
--
-- COPY TRADING, phase 2: what was actually placed for whom.
--
-- copy.orders is the ledger of every attempt, including the ones that never
-- reached the exchange. A subscriber whose trade was skipped for insufficient
-- funds needs to see that as much as a fill, and "no row" is indistinguishable
-- from "the worker never ran".
--
-- IDEMPOTENCY lives here. The worker polls and can be restarted mid-fan-out, so
-- the unique index on (subscription_id, intent_id) is what stops a restart
-- placing a second order for an intent already handled. It is a constraint
-- rather than a check-then-insert because two workers racing would both pass
-- the check.
create table if not exists copy.orders (
  id              bigserial primary key,
  subscription_id bigint not null references copy.subscriptions (id) on delete cascade,
  intent_id       bigint not null references bots.trade_intents (id) on delete cascade,

  kind            text not null check (kind in ('enter', 'exit', 'reprice')),
  token_id        text not null,
  side            text not null,

  status          text not null
                    check (status in ('placed', 'filled', 'partial', 'rejected', 'skipped')),
  -- Why nothing was placed. Null on anything that reached the exchange.
  skip_reason     text,

  requested_usd   numeric,
  limit_price     numeric,
  clob_order_id   text,
  filled_shares   numeric,
  avg_price       numeric,

  error           text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),

  unique (subscription_id, intent_id)
);

create index if not exists copy_orders_sub_idx
  on copy.orders (subscription_id, created_at desc);

-- What a subscriber currently holds BECAUSE of a bot, which is not the same as
-- what their wallet holds: they may have bought the same market themselves, and
-- selling that by hand must not look like the copied position closing.
create table if not exists copy.positions (
  id              bigserial primary key,
  subscription_id bigint not null references copy.subscriptions (id) on delete cascade,
  token_id        text not null,
  bot_trade_id    bigint references bots.trades (id) on delete set null,

  shares          numeric not null default 0,
  cost_usd        numeric not null default 0,
  avg_price       numeric,

  -- TRUE once the bot stops managing it: the subscriber sold or cancelled by
  -- hand, or unsubscribed. One flag behind both, because both mean the same
  -- thing to the fan-out -- skip this position -- and two flags would drift.
  detached        bool not null default false,

  opened_at       timestamptz not null default now(),
  updated_at      timestamptz not null default now(),
  unique (subscription_id, token_id)
);

create index if not exists copy_positions_open_idx
  on copy.positions (subscription_id) where shares > 0 and not detached;

-- How far the fan-out has read. A single row: the worker polls
-- `where id > last_intent_id` rather than listening, because LISTEN needs a
-- durable session and this database is reached through a transaction pooler.
create table if not exists copy.fanout_cursor (
  id             int primary key default 1 check (id = 1),
  last_intent_id bigint not null default 0,
  updated_at     timestamptz not null default now()
);
insert into copy.fanout_cursor (id, last_intent_id) values (1, 0)
  on conflict (id) do nothing;

revoke all on all tables in schema copy from anon, authenticated;
revoke all on all sequences in schema copy from anon, authenticated;
