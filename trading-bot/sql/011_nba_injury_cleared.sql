-- trading-bot/sql/011_nba_injury_cleared.sql
--
-- A player who drops off the injury report used to be deleted, which threw away
-- the only evidence that he is BACK. Absence from a box score cannot show that:
-- an injured player is often not listed at all, and neither is a waived one. The
-- row now stays, stamped with when he was cleared, and is purged after 30 days.
alter table sports.nba_injuries add column if not exists cleared_at timestamptz;
