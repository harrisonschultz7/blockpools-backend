-- trading-bot/sql/004_totals_schema.sql
--
-- Argo-7: NFL TOTALS. Additive to 001/003 -- nothing here changes a table the
-- moneyline bot (Adam-7) reads, so the two run side by side off one feature
-- store and one depth recorder.
--
-- The same rule still governs everything below: rows that feed a forecast are
-- APPEND-ONLY and carry asof_ts. Tables that mirror finished games upsert.

-- == Latent gap in 001 ======================================================
-- ingest/nflverse.js aggregatePbp() has emitted def_pass_epa / def_rush_epa /
-- def_pass_rate since it was written, but 001 never declared them, so the
-- insert depended on those columns having been added to the live database by
-- hand. Declaring them here makes a fresh database reproduce the running one.
alter table sports.nfl_team_game_stats add column if not exists def_pass_epa  numeric;
alter table sports.nfl_team_game_stats add column if not exists def_rush_epa  numeric;
alter table sports.nfl_team_game_stats add column if not exists def_pass_rate numeric;

-- == Polymarket totals registry =============================================
-- Kept separate from sports.pm_markets rather than bolted onto it. A moneyline
-- is one market per game; a total is 25-42 markets per game (one binary per
-- line), so the grain is different and a shared table would force every
-- moneyline query to start filtering by line.
--
-- liquidity_num is stored because it picks the line we trade: the deepest book.
-- Verified against the live API -- the deepest line is always the one priced
-- nearest 50/50 (bid/ask 0.47-0.53 on every game checked), so "deepest" and
-- "closest to a coin flip" select the same market in practice.
--
-- fee_type / fee_rate are recorded per market, NOT assumed. Checked live:
-- totals carry feeType zero_fees (rate 0) while game moneylines carry
-- sports_fees_v3 (rate 0.05, taker-only). That is a real structural edge for a
-- strategy built on round-tripping, and it is Polymarket's to change at any
-- time, so the paper filler reads these columns instead of trusting a constant.
create table if not exists sports.pm_totals_markets (
  condition_id    text primary key,
  slug            text,
  question        text,
  game_id         text references sports.nfl_games (game_id),
  line            numeric not null,
  over_token_id   text,
  under_token_id  text,
  liquidity_num   numeric,
  volume_num      numeric,
  tick_size       numeric,
  min_order_usd   numeric,
  fee_type        text,
  fee_rate        numeric,
  end_date        timestamptz,
  closed          boolean not null default false,
  map_confidence  text,
  first_seen      timestamptz not null default now(),
  last_seen       timestamptz not null default now()
);
create index if not exists pm_totals_game_idx on sports.pm_totals_markets (game_id, liquidity_num desc);
create index if not exists pm_totals_line_idx on sports.pm_totals_markets (game_id, line);

-- == Scheme / play-style tendencies per team-game ============================
-- Built by ingest/ftn.js from nflverse ftn_charting joined to play-by-play on
-- (nflverse_game_id, nflverse_play_id). FTN is the only free source that charts
-- HOW a play was run; pbp supplies who had the ball and the game state.
--
-- NEUTRAL SCRIPT ONLY. Raw tendencies are contaminated by the scoreboard -- a
-- trailing team runs more no-huddle and throws more, which is the deficit
-- talking, not its scheme. Every rate here is measured on plays inside the
-- win-probability / quarter / margin band in config.scheme.neutral, and
-- neutral_plays records how much sample survived so a thin game can be shrunk
-- rather than trusted.
--
-- What is NOT here: coverage shell (zone vs man). It is absent from FTN and from
-- every other free source, so the scheme factor is built on the pressure, tempo
-- and personnel axes instead. Stated explicitly because the gap is easy to
-- forget once the numbers look plausible.
create table if not exists sports.nfl_team_scheme_game (
  game_id            text not null,
  team               text not null,
  opponent           text not null,
  season             int  not null,
  week               int  not null,

  -- offensive tendencies, measured while this team had the ball
  off_neutral_plays  int,
  off_motion_rate    numeric,
  off_pa_rate        numeric,   -- play action, dropbacks only
  off_screen_rate    numeric,
  off_rpo_rate       numeric,
  off_no_huddle_rate numeric,
  off_shotgun_rate   numeric,   -- qb_location other than under centre
  off_backfield_avg  numeric,
  off_epa_neutral    numeric,   -- the fit target

  -- defensive tendencies, measured while this team was the defence
  def_neutral_plays  int,
  def_blitz_rate     numeric,   -- n_blitzers above zero, dropbacks only
  def_rushers_avg    numeric,   -- dropbacks only
  def_box_avg        numeric,   -- rows with a charted box count only
  def_heavy_box_rate numeric,   -- box of 7 or more

  -- RAW NUMERATORS AND DENOMINATORS, as {plays, dropbacks, motion_n, ...}.
  -- The rate columns above are for eyeballing one game; the feature layer
  -- aggregates a team across games from THESE, because averaging per-game
  -- rates weights a 6-neutral-play game exactly as heavily as a 53-play one.
  -- Measured on 2026 weeks 1-2: the neutral band keeps 60% of charted plays,
  -- median 39 per team-game, but the tail runs down to single digits.
  counts             jsonb,

  computed_at        timestamptz not null default now(),
  primary key (game_id, team)
);
create index if not exists nfl_scheme_team_idx on sports.nfl_team_scheme_game (team, season, week);

-- == Totals forecast rows ===================================================
-- The totals analogue of sports.features. Separate table because the factor set
-- differs and, more importantly, because a totals forecast is expressed in
-- POINTS first: the model reads the market's implied mean total, adjusts it in
-- points, and only then converts back to a probability. Storing
-- implied_mean_total and model_mean_total means a past decision can be read in
-- the units it was actually made in.
create table if not exists sports.features_totals (
  id                 bigserial primary key,
  game_id            text not null,
  condition_id       text not null,
  asof_ts            timestamptz not null default now(),
  line               numeric not null,
  p_market           numeric not null,   -- P(over), from the recorded book
  sigma              numeric not null,   -- total-points SD used for the mapping
  implied_mean_total numeric not null,
  f_weather          numeric,            -- every f_* is in POINTS, signed +over
  f_scheme           numeric,
  f_rest             numeric,
  f_pace             numeric,
  f_injury           numeric,
  f_situational      numeric,
  points_delta       numeric,            -- weighted, residualised, pre-cap
  model_mean_total   numeric,
  delta              numeric,            -- probability points, post-cap
  p_fair             numeric,
  confidence         numeric,
  inputs             jsonb,
  model_version      text,
  created_at         timestamptz not null default now()
);
create index if not exists features_totals_game_idx on sports.features_totals (game_id, asof_ts desc);

-- == Bot ledger reuse =======================================================
-- bots.trades / positions / decisions / nav_history are bot_id-scoped already,
-- so Argo-7 shares them. Three columns are added because a totals trade is not
-- fully described by the moneyline shape: which LINE was traded, which market
-- type it was, and which totals forecast row justified it.
alter table bots.trades    add column if not exists line numeric;
alter table bots.trades    add column if not exists market_type text not null default 'moneyline';
alter table bots.trades    add column if not exists feature_totals_id bigint references sports.features_totals (id);
alter table bots.positions add column if not exists line numeric;
alter table bots.positions add column if not exists market_type text not null default 'moneyline';
alter table bots.decisions add column if not exists line numeric;
alter table bots.decisions add column if not exists market_type text not null default 'moneyline';
alter table bots.decisions add column if not exists feature_totals_id bigint references sports.features_totals (id);

-- A totals bot looks at one game across many lines. The policy still takes at
-- most one line per game, so the one-position-per-game assumption holds, but
-- the index should reflect the new grain.
create index if not exists bots_trades_market_idx on bots.trades (bot_id, market_type, opened_at desc);

-- Same denial as 003. The default privileges set there already cover new tables
-- in these schemas; restated so this file is safe to run standalone.
revoke all on sports.pm_totals_markets, sports.nfl_team_scheme_game,
              sports.features_totals from anon, authenticated;

-- == bots.limit_orders -- second latent gap in the committed schema ==========
-- The resting-exit engine (src/exec/limits.js) has written to this table since it
-- was built, but no sql/ file ever declared it: it was created directly against
-- the live database. Declaring it here, idempotently, means a fresh database
-- reproduces the running one instead of failing on the first exit order.
--
-- THE RESTING SELL IS THE PRODUCT. A sportsbook makes you hold to settlement;
-- this row is what lets the bot name a price and get paid on an in-play swing
-- instead. It is repriced every tick before kickoff and frozen once the ball is
-- in the air, because in play the model has no live feed and would only be
-- chasing the price.
create table if not exists bots.limit_orders (
  id            bigserial primary key,
  bot_id        text not null references bots.bot (id),
  trade_id      bigint references bots.trades (id),
  game_id       text not null,
  token_id      text not null,
  side          text not null,
  action        text not null,                     -- 'SELL'
  limit_price   numeric not null,
  original_price numeric,
  shares        numeric not null,
  status        text not null default 'open',       -- open | filled | cancelled
  reprice_count int  not null default 0,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  filled_at     timestamptz,
  fill_price    numeric,
  fill_detail   jsonb,
  closed_reason text
);
create index if not exists bots_limit_orders_open_idx
  on bots.limit_orders (bot_id, status, game_id);

-- Totals need the condition_id: a game has one moneyline but 25-42 totals
-- markets, so game_id alone cannot say which market an order belongs to. Without
-- it the closed-market check would resolve a totals order against the game's
-- MONEYLINE market, which is usually right by accident and silently wrong when
-- the two close at different times.
alter table bots.limit_orders add column if not exists condition_id text;
alter table bots.limit_orders add column if not exists market_type text not null default 'moneyline';
alter table bots.limit_orders add column if not exists line numeric;

revoke all on bots.limit_orders from anon, authenticated;

-- == bots.bot display columns -- third latent gap in the committed schema =====
-- src/routes/bots.ts has selected market_scope and description since the
-- endpoint was written, and 002 never declared either: both were added to the
-- live database by hand and backfilled for Adam-7 with a manual UPDATE. Declared
-- here so a fresh database serves the same API, and run/tick-totals.js now writes
-- them from config on every tick so a NEW bot never needs hand-written SQL to
-- appear correctly on the leaderboard.
alter table bots.bot add column if not exists market_scope text;
alter table bots.bot add column if not exists description  text;
