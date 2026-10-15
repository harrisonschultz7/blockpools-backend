-- trading-bot/sql/005_passing_schema.sql
--
-- Cash-7 (working name): NFL QB PASSING YARDS props. Additive to 001/004 --
-- nothing here changes a table Adam-7 (moneyline) or Argo-7 (totals) reads, so
-- all three run off one feature store, one depth recorder and one bot ledger.
--
-- Same rule as 004: rows that feed a forecast are APPEND-ONLY and carry asof_ts;
-- tables that mirror finished games UPSERT. This file mirrors the live migration
-- passing_schema_005 so a fresh database reproduces the running one.

-- == Player-week box scores -> sports.nfl_player_week =========================
-- Source: nflverse stats_player release, file stats_player_week_<season>.csv
-- (the old player_stats_<season>.csv path was retired upstream; this is current).
-- One row per player per finished game, projected at ingest to the columns three
-- things need, exactly like the pbp aggregation:
--   1. QB RECENT FORM  -- passing_yards / attempts / passing_epa / cpoe.
--   2. OPPONENT PASS D  -- pass yards/attempts ALLOWED = sum of opposing QB rows
--      (pass-only!)        vs that defteam. No run-defense leakage, which is the
--                          whole point of the matchup factor.
--   3. WR-INJURY WEIGHT -- target_share / air_yards_share / wopr, so a receiver
--                          ruled out subtracts the exact share vacated.
-- player_id is the gsis id, so it joins to nfl_games.home_qb_id / away_qb_id --
-- the projected starter is already named by the schedule (QB only; RB/WR later).
create table if not exists sports.nfl_player_week (
  game_id                   text not null,
  player_id                 text not null,            -- gsis id
  player_display_name       text,
  position                  text,
  team                      text,
  opponent_team             text,
  season                    int  not null,
  week                      int  not null,
  season_type               text,
  completions               numeric,
  attempts                  numeric,
  passing_yards             numeric,
  passing_tds               numeric,
  passing_interceptions     numeric,
  sacks_suffered            numeric,
  passing_air_yards         numeric,
  passing_yards_after_catch numeric,
  passing_epa               numeric,
  passing_cpoe              numeric,
  carries                   numeric,
  rushing_yards             numeric,
  rushing_tds               numeric,
  receptions                numeric,
  targets                   numeric,
  receiving_yards           numeric,
  receiving_tds             numeric,
  target_share              numeric,
  air_yards_share           numeric,
  wopr                      numeric,
  updated_at                timestamptz not null default now(),
  primary key (game_id, player_id)
);
create index if not exists nfl_player_week_player_idx on sports.nfl_player_week (player_id, season, week);
create index if not exists nfl_player_week_def_idx    on sports.nfl_player_week (opponent_team, position, season, week);
create index if not exists nfl_player_week_team_idx   on sports.nfl_player_week (team, position, season, week);

-- == Polymarket passing-yards prop registry -> sports.pm_passing_markets ======
-- Clone of pm_totals_markets, one row per (player, line). ~6-12 lines per QB per
-- game; we trade only the deepest/central line (nearest 50/50), so liquidity_num
-- and tick/fee are recorded per market, never assumed -- props seed near kickoff
-- and tail lines are empty books.
create table if not exists sports.pm_passing_markets (
  condition_id    text primary key,
  slug            text,
  question        text,
  game_id         text references sports.nfl_games (game_id),
  player_id       text,                         -- gsis id when resolvable
  player_name     text,                         -- as the market names them
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
create index if not exists pm_passing_game_idx   on sports.pm_passing_markets (game_id, liquidity_num desc);
create index if not exists pm_passing_player_idx on sports.pm_passing_markets (player_id, line);

-- == Passing forecast rows -> sports.features_passing =========================
-- The passing analogue of features_totals, expressed in YARDS first: read the
-- market implied mean (the central line IS the market median), adjust in yards,
-- then convert to P(over) through a normal with sigma.
-- Every f_* is in YARDS, signed + = toward Over. Priors (calibration will move
-- them, as it zeroed Argo scheme/rest and promoted weather):
--   f_matchup 30%  opponent pass-D, PER-ATTEMPT, pass-only
--   f_injury  25%  WR availability, target-share weighted
--   f_weather 22%  wind first, then heavy rain
--   f_form    13%  QB last 2-3 games vs market expectation
--   f_script  10%  implied attempts from Adam win-prob + Argo total
create table if not exists sports.features_passing (
  id                 bigserial primary key,
  game_id            text not null,
  condition_id       text not null,
  player_id          text,
  asof_ts            timestamptz not null default now(),
  line               numeric not null,
  p_market           numeric not null,   -- P(over), from the recorded book
  sigma              numeric not null,   -- passing-yards SD used for the mapping
  implied_mean_yards numeric not null,
  f_matchup          numeric,
  f_injury           numeric,
  f_weather          numeric,
  f_form             numeric,
  f_script           numeric,
  yards_delta        numeric,            -- weighted, residualised, pre-cap
  model_mean_yards   numeric,
  delta              numeric,            -- probability points, post-cap
  p_fair             numeric,
  confidence         numeric,
  inputs             jsonb,
  model_version      text,
  created_at         timestamptz not null default now()
);
create index if not exists features_passing_game_idx   on sports.features_passing (game_id, asof_ts desc);
create index if not exists features_passing_player_idx on sports.features_passing (player_id, asof_ts desc);

-- Bot ledger reuse: trades/positions/decisions/limit_orders already carry
-- market_type + line + condition_id (004); one new FK to the forecast row.
alter table bots.trades    add column if not exists feature_passing_id bigint references sports.features_passing (id);
alter table bots.decisions add column if not exists feature_passing_id bigint references sports.features_passing (id);

revoke all on sports.nfl_player_week, sports.pm_passing_markets,
              sports.features_passing from anon, authenticated;
