-- src/db/migrations/2026-05-28_users_bio_favorite_team.sql
--
-- Adds two profile-personalization columns to the `users` table so the
-- create-profile modal and the desktop profile page can persist them.
--
--   1. users.bio            (text, ≤280 chars enforced at the API layer)
--   2. users.favorite_team  (text, format "<sport>:<CODE>" — e.g. "nfl:PHI")
--
-- Both are nullable: existing rows stay valid, and the picker can clear
-- the value at any time. Idempotent — safe to re-run on Supabase.
--
-- Apply: paste into the Supabase SQL editor and run.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS bio text;

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS favorite_team text;

-- favorite_team is queried in a "users with team X" pattern (e.g. league
-- chat / leaderboard filters). Tiny btree keeps that fast without bloating
-- the row. We lowercase-index so the comparison can be case-insensitive
-- without needing to normalize at write time.
CREATE INDEX IF NOT EXISTS users_favorite_team_idx
  ON public.users (lower(favorite_team))
  WHERE favorite_team IS NOT NULL;
