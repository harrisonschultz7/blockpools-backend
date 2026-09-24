-- trading-bot/sql/001_sports_schema.sql
--
-- Feature store for the NFL trading bot.
--
-- THE RULE THAT MAKES THIS WORK: every row that feeds a forecast carries an
-- asof_ts / observed_at and is APPEND-ONLY. Nothing the model reads is ever
-- updated in place. A silently-corrected injury row, or a book snapshot
-- rewritten after kickoff, turns a backtest into fiction -- the most common way
-- a sports model looks brilliant offline and loses money live.
--
-- Tables that mirror an external source-of-truth (nfl_games, team_game_stats,
-- snap_counts) DO upsert, because they only describe games that already
-- finished. They are never read for a game whose kickoff has not passed.

create schema if not exists sports;

-- == Canonical games (mirror of nflverse schedules/games.csv) ================
-- Carries the CLOSING moneyline/spread/total -- our CLV ground truth. Odds are
-- null until the game is priced and fill in as the week approaches.
create table if not exists sports.nfl_games (
  game_id             text primary key,
  season              int  not null,
  week                int  not null,
  game_type           text,
  gameday             date,
  weekday             text,
  gametime            text,
  kickoff             timestamptz,
  away_team           text not null,
  home_team           text not null,
  away_score          int,
  home_score          int,
  result              int,
  away_rest           int,
  home_rest           int,
  div_game            boolean,
  roof                text,
  surface             text,
  temp                int,
  wind                int,
  away_moneyline      int,
  home_moneyline      int,
  spread_line         numeric,
  total_line          numeric,
  away_qb_id          text,
  home_qb_id          text,
  stadium_id          text,
  stadium             text,
  closing_captured_at timestamptz,
  updated_at          timestamptz not null default now()
);
create index if not exists nfl_games_season_week_idx on sports.nfl_games (season, week);
create index if not exists nfl_games_kickoff_idx     on sports.nfl_games (kickoff);

-- == Team-game aggregates derived from play-by-play =========================
-- One row per (game, team). Feeds momentum and the pace x defense matchup.
create table if not exists sports.nfl_team_game_stats (
  game_id          text not null,
  team             text not null,
  opponent         text not null,
  season           int  not null,
  week             int  not null,
  is_home          boolean not null,
  off_plays        int,
  off_epa_per_play numeric,
  off_success_rate numeric,
  off_pass_epa     numeric,
  off_rush_epa     numeric,
  off_pass_rate    numeric,
  def_plays        int,
  def_epa_per_play numeric,
  def_success_rate numeric,
  sec_per_play     numeric,
  plays_total      int,
  points_for       int,
  points_against   int,
  computed_at      timestamptz not null default now(),
  primary key (game_id, team)
);
create index if not exists nfl_tgs_team_idx on sports.nfl_team_game_stats (team, season, week);

-- == Snap counts -- the injury BASELINE =====================================
-- What makes "are the Rams fine without Nacua?" answerable: who actually took
-- the field, at what share. The injury report says Active; snap_pct says he
-- played 38% of downs, which the report cannot tell you.
create table if not exists sports.nfl_snap_counts (
  game_id       text not null,
  pfr_player_id text not null,
  player        text,
  position      text,
  team          text not null,
  opponent      text,
  season        int  not null,
  week          int  not null,
  offense_snaps int,
  offense_pct   numeric,
  defense_snaps int,
  defense_pct   numeric,
  st_pct        numeric,
  primary key (game_id, pfr_player_id)
);
create index if not exists nfl_snaps_player_idx on sports.nfl_snap_counts (pfr_player_id, season, week);
create index if not exists nfl_snaps_team_idx   on sports.nfl_snap_counts (team, season, week);

-- == Weekly injury report -- APPEND-ONLY with observed_at ===================
-- The nflverse CSV is re-published in place with no report date, so the
-- Wed -> Fri progression of a Questionable tag is NOT recoverable historically.
-- Stamping observed_at on every pull rebuilds that progression going forward,
-- which is what the live bot actually needs.
create table if not exists sports.nfl_injuries (
  id                    bigserial primary key,
  season                int  not null,
  week                  int  not null,
  team                  text not null,
  gsis_id               text,
  full_name             text,
  position              text,
  report_status         text,
  report_primary_injury text,
  practice_status       text,
  observed_at           timestamptz not null default now()
);
create index if not exists nfl_inj_lookup_idx on sports.nfl_injuries (season, week, team, observed_at desc);

-- == Gameday inactives (ESPN, ~90 min pre-kickoff) -- APPEND-ONLY ===========
create table if not exists sports.nfl_inactives (
  id          bigserial primary key,
  game_id     text not null,
  team        text not null,
  player      text,
  position    text,
  espn_id     text,
  observed_at timestamptz not null default now()
);
create index if not exists nfl_inactives_game_idx on sports.nfl_inactives (game_id, observed_at desc);

-- == Stadium coordinates (for weather forecasts) ============================
create table if not exists sports.stadiums (
  stadium_id text primary key,
  name       text,
  team       text,
  lat        numeric not null,
  lon        numeric not null,
  roof       text,
  tz         text
);

-- == Weather FORECAST snapshots -- APPEND-ONLY ==============================
-- nflverse temp/wind are observed post-hoc (0 of 240 future games populated),
-- so forecasts must be recorded ourselves, at the time we saw them.
create table if not exists sports.nfl_weather (
  id                bigserial primary key,
  game_id           text not null,
  asof_ts           timestamptz not null default now(),
  forecast_temp_f   numeric,
  forecast_wind_mph numeric,
  precip_prob       numeric,
  source            text not null default 'open-meteo'
);
create index if not exists nfl_weather_game_idx on sports.nfl_weather (game_id, asof_ts desc);

-- == Polymarket market registry =============================================
-- Maps a Polymarket condition to our nflverse game_id. The mapping is the
-- fiddly part (team naming), so it is resolved once and reused.
create table if not exists sports.pm_markets (
  condition_id   text primary key,
  slug           text,
  question       text,
  game_id        text references sports.nfl_games (game_id),
  home_token_id  text,
  away_token_id  text,
  home_outcome   text,
  away_outcome   text,
  end_date       timestamptz,
  closed         boolean not null default false,
  map_confidence text,
  first_seen     timestamptz not null default now(),
  last_seen      timestamptz not null default now()
);
create index if not exists pm_markets_game_idx on sports.pm_markets (game_id);

-- == THE DEPTH RECORDER -- append-only order-book snapshots =================
-- Polymarket's CLOB serves current state only; historical depth cannot be
-- backfilled from anywhere at any price. Every hour this is not running is an
-- hour of backtest fidelity permanently lost. This is why ingest ships first.
--
-- bids/asks hold top levels as [[price,size],...] so the paper filler walks
-- real depth instead of pretending everything fills at the midpoint.
create table if not exists sports.odds_history (
  id            bigserial primary key,
  ts            timestamptz not null default now(),
  condition_id  text not null,
  token_id      text not null,
  game_id       text,
  side          text,
  mid           numeric,
  best_bid      numeric,
  best_ask      numeric,
  spread        numeric,
  bid_depth_usd numeric,
  ask_depth_usd numeric,
  bids          jsonb,
  asks          jsonb
);
create index if not exists odds_hist_game_ts_idx  on sports.odds_history (game_id, ts desc);
create index if not exists odds_hist_token_ts_idx on sports.odds_history (token_id, ts desc);

-- == Point-in-time feature + forecast rows ==================================
-- One row per (game, evaluation): the market anchor, each factor's signed
-- contribution, the capped residual and p_fair, plus the full input payload so
-- any past decision can be explained WITHOUT recomputing it (which would
-- silently use today's data).
create table if not exists sports.features (
  id            bigserial primary key,
  game_id       text not null,
  asof_ts       timestamptz not null default now(),
  p_market      numeric not null,
  f_momentum    numeric,
  f_matchup     numeric,
  f_injury      numeric,
  f_situational numeric,
  delta         numeric,
  p_fair        numeric,
  confidence    numeric,
  inputs        jsonb,
  model_version text,
  created_at    timestamptz not null default now()
);
create index if not exists features_game_idx on sports.features (game_id, asof_ts desc);
