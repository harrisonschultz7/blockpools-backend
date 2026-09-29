-- trading-bot/sql/007_copy_schema.sql
--
-- COPY TRADING, phase 1: who is copying what, and on what terms.
--
-- The sleeve model. A subscriber allocates CAPITAL TO A STRATEGY, not to a
-- wallet: one Polymarket wallet underneath, N notional sleeves on top. Copying
-- two bots with $500 each on a $900 balance is over-allocation, and that is
-- allowed on purpose -- balances move, and blocking it at setup would be worse
-- than failing one trade with a reason the subscriber can see.
--
-- Sleeve NAV = basis_usd + realized_pnl + unrealised on that sleeve's open
-- positions. It COMPOUNDS, because the bot sizes off its own live NAV: a sleeve
-- frozen at its opening basis would drift below the bot's percentage return over
-- a season. Editing basis_usd later acts like a deposit or withdrawal into the
-- sleeve; realized_pnl survives the edit.
create schema if not exists copy;

create table if not exists copy.subscriptions (
  id             bigserial primary key,
  privy_did      text not null,
  -- The wallet that will actually trade. Stored rather than resolved per request
  -- because a subscriber who later links a second wallet must not silently have
  -- their positions move to it.
  wallet_address text not null,
  bot_id         text not null references bots.bot (id),

  status         text not null default 'active'
                   check (status in ('active', 'paused', 'revoked')),

  -- what the subscriber allocated to THIS bot
  basis_usd      numeric not null check (basis_usd > 0),
  -- optional dial on top of the mirrored percentage
  multiplier     numeric not null default 1.0
                   check (multiplier > 0 and multiplier <= 3),
  -- hard ceiling per trade, independent of what the percentage works out to
  max_trade_usd  numeric check (max_trade_usd is null or max_trade_usd > 0),

  realized_pnl   numeric not null default 0,

  -- Set when Privy reports the embedded wallet as delegated, cleared when the
  -- subscriber revokes. Nothing may be signed for a subscription where this is
  -- null -- see the fan-out worker.
  delegated_at   timestamptz,

  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),

  -- One sleeve per bot per subscriber. Copying the same bot twice is not a
  -- feature, it is a double position with half the visibility.
  unique (privy_did, bot_id)
);

create index if not exists copy_subscriptions_bot_idx
  on copy.subscriptions (bot_id) where status = 'active';

-- WHAT THEY AGREED TO, append only.
--
-- Separate from the subscription because the subscription is current state and
-- this is a record of an event. A subscriber who pauses, edits their sleeve and
-- resumes still consented once, on a particular version of the copy, at a
-- particular moment -- and if the terms change, the old acceptance must not be
-- rewritten to look like acceptance of the new ones.
create table if not exists copy.consents (
  id            bigserial primary key,
  privy_did     text not null,
  bot_id        text not null,
  terms_version text not null,
  accepted_at   timestamptz not null default now(),
  ip            text,
  user_agent    text
);

create index if not exists copy_consents_user_idx
  on copy.consents (privy_did, bot_id, accepted_at desc);

-- Server-only, same as sports and bots. These rows say exactly how much money a
-- named person has pointed at a strategy.
revoke all on schema copy from anon, authenticated;
revoke all on all tables in schema copy from anon, authenticated;
revoke all on all sequences in schema copy from anon, authenticated;
alter default privileges in schema copy revoke all on tables from anon, authenticated;
alter default privileges in schema copy revoke all on sequences from anon, authenticated;
