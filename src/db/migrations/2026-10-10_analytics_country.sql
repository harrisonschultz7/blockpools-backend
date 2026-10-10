-- src/db/migrations/2026-10-10_analytics_country.sql
--
-- Adds country: the visitor's ISO country from the edge (/api/geo →
-- x-vercel-ip-country), stamped on every event once it resolves client-side.
-- Until now geo was detected at runtime but never stored, so "Mexico" had to be
-- guessed from locale (es-MX) — wrong for MX users on es-US / en phones.
--
-- Apply in the Supabase SQL editor, then RELOAD PostgREST (below) or the ingest
-- insert fails with "Could not find the 'country' column ... in the schema cache".

ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS country text;

CREATE INDEX IF NOT EXISTS analytics_events_country_idx
  ON public.analytics_events (country, occurred_at)
  WHERE country IS NOT NULL;

NOTIFY pgrst, 'reload schema';
