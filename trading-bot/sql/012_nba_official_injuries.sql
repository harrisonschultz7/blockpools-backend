-- 012: the NBA's OFFICIAL injury report, logged next to the ESPN feed
-- (sports.nba_injuries) so we can measure which source sees news first and
-- whether either beats the Polymarket price (2026-11-23). Research logging
-- only: no model reads these tables yet.
--
-- The league republishes one PDF on 15-minute marks through the day. Only
-- STATUS CHANGES are stored: a row is written when a player's status for a
-- game differs from his last logged one, stamped with the moment WE first saw
-- it (first_seen_at) -- that stamp is the measurement.

create table if not exists sports.nba_official_injuries (
  id             bigserial primary key,
  game_date      date        not null,
  matchup        text,                      -- "MEM@CHA"
  team           text        not null,      -- full team name as printed
  player         text        not null,      -- "Cameron Boozer" (report prints "Boozer, Cameron")
  status         text        not null,      -- Out / Doubtful / Questionable / Probable / Available
  reason         text,
  report_at      timestamptz not null,      -- the report's own timestamp (its ET label)
  report_url     text        not null,
  first_seen_at  timestamptz not null default now()
);
create index if not exists nba_official_injuries_key
  on sports.nba_official_injuries (game_date, team, player, first_seen_at desc);

-- Every report file we have read, so each is parsed once.
create table if not exists sports.nba_official_injury_reports (
  report_url  text primary key,
  report_at   timestamptz not null,
  fetched_at  timestamptz not null default now(),
  rows_parsed int,
  changes     int
);
