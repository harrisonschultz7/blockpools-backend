-- ============================================================================
-- Social Tags: per-(user, league) bettor categorization.
--
-- Run this in the Supabase SQL editor. It is idempotent (safe to re-run).
--
-- Why: the "social tags" feature shows, on each market, an anonymous count of
-- what KIND of bettors are on each side (🔥 Hot / 🔮 Sharp / 🐋 Whale). Hot and
-- Sharp are properties of the USER within a LEAGUE (derived from their betting
-- history), so they are materialized here once per refresh cron rather than
-- recomputed on every market read. Whale is a property of a single POSITION on a
-- single market ($30+ of own money on that side) and is NOT stored here — it is
-- computed live in the per-market aggregate endpoint.
--
-- One row per (user_address, league). A user can qualify as both Hot and Sharp;
-- we store only the higher-priority of the two (Hot > Sharp), because at render
-- time the market-level collapse is Whale > Hot > Sharp and Whale is layered on
-- top per-position. So the only distinction this table needs to preserve is
-- "is this user Hot, else Sharp, else untagged" per league.
--
-- Tag definitions (computed by POST /api/social-tags/refresh):
--   hot   = the user's 3 most-recent RESOLVED picks in the league were all wins.
--   sharp = 30-day ROI > 10% in the league with >= 5 settled trades.
--
-- Attribution uses effective_user_address (= COALESCE(beneficiary_address,
-- user_address)) so promo/free-bet trades count toward the real beneficiary.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.user_tags_by_league (
  user_address text        NOT NULL,
  league       text        NOT NULL,
  tag          text        NOT NULL CHECK (tag IN ('hot', 'sharp')),
  computed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_address, league)
);

-- Read pattern A (per-market aggregate): "give me the tag for these users in
-- this league" — covered by the PK (user_address, league).
-- Read pattern B (admin / future "list all hot users in NBA"): filter by
-- (league, tag) — add a secondary index.
CREATE INDEX IF NOT EXISTS user_tags_by_league_league_tag_idx
  ON public.user_tags_by_league (league, tag);

COMMENT ON TABLE public.user_tags_by_league IS
  'Materialized 🔥 Hot / 🔮 Sharp bettor tags per (user, league). Refreshed by POST /api/social-tags/refresh. Whale is per-position and computed live, not stored here.';
