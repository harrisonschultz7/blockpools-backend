-- trading-bot/sql/011_nba_injury_cleared.sql
--
-- A player who drops off the injury report used to be deleted, which threw away
-- the only evidence that he is BACK. Absence from a box score cannot show that:
-- an injured player is often not listed at all, and neither is a waived one. The
-- row now stays, stamped with when he was cleared, and is purged after 30 days.
alter table sports.nba_injuries add column if not exists cleared_at timestamptz;

-- nba_injuries holds only each player's latest status, so a review could not tell
-- what the models knew before a tip (2026-11-05: Goodwin and Santos both missed
-- games, and neither could be checked). One row per change, written by the ingest.
create table if not exists sports.nba_injury_events (
  id               bigserial primary key,
  player_id        text not null,
  team             text,
  player           text,
  old_status       text,            -- null: newly on the report
  new_status       text not null,   -- CLEARED: off the report
  short_comment    text,
  espn_updated_at  timestamptz,
  observed_at      timestamptz not null default now()
);
create index if not exists nba_injury_events_team_idx on sports.nba_injury_events (team, observed_at desc);
create index if not exists nba_injury_events_player_idx on sports.nba_injury_events (player_id, observed_at desc);
-- Same lockdown as 003_grants.sql: nothing in sports is readable by the public.
revoke all on sports.nba_injury_events from anon, authenticated;
