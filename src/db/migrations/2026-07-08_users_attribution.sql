-- src/db/migrations/2026-07-08_users_attribution.sql
--
-- Meta attribution: persist first-touch ad attribution onto each user so we can
-- (a) analyse which campaigns/ads produce funded users (first traders), and
-- (b) include the ad id in the server-side FirstTrade Conversions API event.
--
-- Additive + idempotent. No existing column is touched; all new columns are
-- nullable and default NULL. Safe to run on production and re-runnable.
--
-- Apply: paste into the Supabase SQL editor and run (or your normal migration
-- runner). No view rebuild required.
--
-- Column meaning (first-touch wins; populated by the visitor_id->wallet bridge):
--   attributed_utm_source    e.g. 'fb'
--   attributed_utm_campaign  Meta campaign id (numeric, from utm_campaign)
--   attributed_utm_content   Meta ad id (numeric, from utm_content)
--   attributed_utm_term      Meta id (from utm_term)
--   attributed_landing       landing path at first paid touch (e.g. '/app')
--   attributed_at            timestamp of that first touch (NULL = not attributed)

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS attributed_utm_source   text,
  ADD COLUMN IF NOT EXISTS attributed_utm_campaign text,
  ADD COLUMN IF NOT EXISTS attributed_utm_content  text,
  ADD COLUMN IF NOT EXISTS attributed_utm_term     text,
  ADD COLUMN IF NOT EXISTS attributed_landing      text,
  ADD COLUMN IF NOT EXISTS attributed_at           timestamptz;

-- Lets the funded-audience / campaign-performance queries filter by ad id cheaply.
CREATE INDEX IF NOT EXISTS users_attributed_utm_content_idx
  ON public.users (attributed_utm_content)
  WHERE attributed_utm_content IS NOT NULL;
