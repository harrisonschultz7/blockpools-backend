-- src/db/migrations/2026-06-01_analytics_in_app_browser.sql
--
-- Adds in_app_browser: the embedded/in-app browser the session is in
-- (e.g. "Facebook", "Instagram", "TikTok"), or NULL for a normal browser.
-- Lets us measure sign-in friction specifically for Meta-ad WebView traffic.
-- (The full user-agent is captured once per session inside metadata->entry->ua.)
--
-- Apply in the Supabase SQL editor. IMPORTANT: after this runs you MUST reload
-- PostgREST's schema cache or the analytics insert will start failing with
-- "Could not find the 'in_app_browser' column ... in the schema cache" — the
-- ingest goes through the Supabase JS client, which caches the schema.

ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS in_app_browser text;

CREATE INDEX IF NOT EXISTS analytics_events_in_app_browser_idx
  ON public.analytics_events (in_app_browser)
  WHERE in_app_browser IS NOT NULL;

-- Reload PostgREST so the new column is writable immediately.
NOTIFY pgrst, 'reload schema';
