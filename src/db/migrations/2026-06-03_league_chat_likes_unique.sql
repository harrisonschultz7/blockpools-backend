-- src/db/migrations/2026-06-03_league_chat_likes_unique.sql
--
-- Guarantees likes on league-chat posts actually persist.
--
-- The like endpoint upserts with:
--   .upsert({ post_id, profile_id }, { onConflict: "post_id,profile_id" })
-- Postgres requires a UNIQUE (or exclusion) constraint matching that
-- ON CONFLICT target. If it's missing, every upsert errors out — and because
-- the old handler swallowed the error, the like looked saved (optimistic UI)
-- but was never written. Navigating away and back showed it gone.
--
-- This adds the unique constraint (one like per user per post) so the upsert
-- resolves, plus indexes the per-post like_count aggregate and the upcoming
-- notifications feature will both hit. Idempotent — safe to re-run on Supabase.
--
-- Notifications-readiness: profile_id holds the liker's users.id, so a future
-- "X liked your post" notification joins league_chat_likes.profile_id ->
-- users.id for the actor's name/avatar, and derives the recipient from
-- league_chat_posts.author_id. The (post_id, created_at) and (profile_id,
-- created_at) indexes below serve "recent likes on my posts" and "things I
-- liked" time-ordered lookups respectively.
--
-- Apply: paste into the Supabase SQL editor and run.

-- De-dupe first, in case any duplicate (post_id, profile_id) rows already exist —
-- otherwise the unique constraint can't be created. Keep the earliest row.
DELETE FROM public.league_chat_likes a
USING public.league_chat_likes b
WHERE a.ctid > b.ctid
  AND a.post_id = b.post_id
  AND a.profile_id = b.profile_id;

-- One like per user per post. This is the ON CONFLICT target the API relies on.
-- Guarded so re-running is safe (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'league_chat_likes_post_profile_uniq'
  ) THEN
    ALTER TABLE public.league_chat_likes
      ADD CONSTRAINT league_chat_likes_post_profile_uniq UNIQUE (post_id, profile_id);
  END IF;
END $$;

-- Make sure created_at is always populated — notifications order by it and use
-- it as the "new since you last looked" boundary. Backfill any nulls first.
UPDATE public.league_chat_likes SET created_at = now() WHERE created_at IS NULL;

ALTER TABLE public.league_chat_likes
  ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE public.league_chat_likes
  ALTER COLUMN created_at SET NOT NULL;

-- Per-post like counts + "recent likes on this post" (notifications feed).
CREATE INDEX IF NOT EXISTS league_chat_likes_post_created_idx
  ON public.league_chat_likes (post_id, created_at DESC);

-- "Posts this user has liked" + self-like exclusion in notification queries.
CREATE INDEX IF NOT EXISTS league_chat_likes_profile_idx
  ON public.league_chat_likes (profile_id);
