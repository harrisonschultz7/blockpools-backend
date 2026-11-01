-- trading-bot/sql/010_nba_schema.sql
--
-- NBA (Axis-3). Source of truth is ESPN's public site API -- the NBA has no
-- nflverse equivalent. Team codes are POLYMARKET's (slug codes upper-cased:
-- GSW, NYK, NOP, SAS, UTA, WAS), so a market maps to a game without an alias
-- table on the hot path; ESPN's abbreviations are translated once, at ingest.
--
-- game_id = 'NBA_' || ESPN event id. Stable, unique, and never collides with an
-- nflverse id (2026_08_TEN_CIN).

create table if not exists sports.nba_games (
  game_id       text primary key,
  espn_id       text not null unique,
  season        int  not null,          -- season START year: 2026 = 2026-27
  season_type   int  not null,          -- ESPN: 1 pre, 2 regular, 3 post, 5 play-in
  game_date     date not null,          -- US-Eastern calendar date
  kickoff       timestamptz not null,   -- tip-off; named kickoff so shared bot code reads it
  away_team     text not null,
  home_team     text not null,
  away_score    int,                    -- null until FINAL (never a live score)
  home_score    int,
  status        text,                   -- ESPN STATUS_SCHEDULED / _IN_PROGRESS / _FINAL / _POSTPONED
  national_tv   text,                   -- comma list of NATIONAL broadcasters, null if local only
  neutral_site  boolean not null default false,
  -- The market's HOME win probability at tip (last recorded mid before tip),
  -- written by run/tick-nba.js. This is the calibration set: the books behind
  -- it are pruned a few days after the final (run/daily.js), the number is not.
  closing_home_mid numeric,
  updated_at    timestamptz not null default now()
);
create index if not exists nba_games_kickoff_idx on sports.nba_games (kickoff);
create index if not exists nba_games_team_idx    on sports.nba_games (home_team, kickoff);
create index if not exists nba_games_away_idx    on sports.nba_games (away_team, kickoff);

-- One row per player per FINAL game, from the box score. didNotPlay rows are
-- kept (minutes 0, dnp true): "who sat" is the whole input to the injury factor.
create table if not exists sports.nba_player_games (
  game_id     text not null references sports.nba_games (game_id),
  player_id   text not null,            -- ESPN athlete id
  team        text not null,
  player      text,
  position    text,
  starter     boolean,
  dnp         boolean not null default false,
  dnp_reason  text,
  minutes     numeric,
  points      int,
  plus_minus  numeric,
  fga         int,
  fg3a        int,
  fta         int,
  oreb        int,
  tov         int,
  primary key (game_id, player_id)
);
create index if not exists nba_pg_player_idx on sports.nba_player_games (player_id);
create index if not exists nba_pg_team_idx   on sports.nba_player_games (team);

-- The CURRENT injury report, one row per listed player. status_changed_at is
-- OURS: the first time we saw this status. It is what the injury factor's
-- freshness discount keys on -- a status the market has known for six hours is
-- priced; one that flipped twenty minutes ago may not be. A player who drops off
-- the report is deleted (cleared to play).
create table if not exists sports.nba_injuries (
  player_id          text primary key,
  team               text not null,
  player             text,
  status             text not null,     -- Out / Day-To-Day / Questionable / Doubtful / Probable
  short_comment      text,
  espn_updated_at    timestamptz,
  status_changed_at  timestamptz not null default now(),
  observed_at        timestamptz not null default now()
);
create index if not exists nba_injuries_team_idx on sports.nba_injuries (team);

-- NBA moneylines live in sports.pm_markets beside the NFL ones, so the exit
-- manager, the recorder sweep and settle-from-resolution reuse them unchanged.
-- That needs the NFL-only foreign key gone; game_id stays indexed.
do $$
declare c text;
begin
  for c in
    select con.conname from pg_constraint con
      join pg_class rel on rel.oid = con.conrelid
      join pg_namespace n on n.oid = rel.relnamespace
     where n.nspname = 'sports' and rel.relname = 'pm_markets' and con.contype = 'f'
  loop
    execute format('alter table sports.pm_markets drop constraint %I', c);
  end loop;
end $$;

-- EVERY LEAGUE'S GAMES, in the columns shared bot code needs: settlement, CLV,
-- the exit manager, the bots API, model alerts, copy. Those join this view; the
-- NFL-only feature code keeps reading sports.nfl_games.
create or replace view sports.bot_games as
  select game_id, 'NFL'::text as league, season, week, kickoff,
         away_team, home_team, away_score, home_score
    from sports.nfl_games
  union all
  -- NBA "week": MONDAY-SUNDAY (US-Eastern) weeks, week 1 = the week holding
  -- Oct 20 (opening week). The weekly ROI card, trade log and selectivity are
  -- keyed on (season, week), so NBA rides the same code as the NFL bots.
  select game_id, 'NBA'::text, season,
         (((date_trunc('week', game_date::timestamp)::date
            - date_trunc('week', make_date(season, 10, 20)::timestamp)::date) / 7) + 1)::int,
         kickoff, away_team, home_team, away_score, home_score
    from sports.nba_games;

-- Same lockdown as 003_grants.sql: nothing in sports is readable by the public
-- API roles (default privileges cover the tables; the view is explicit).
revoke all on sports.bot_games from anon, authenticated;
