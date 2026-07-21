-- src/db/migrations/2026-07-21_users_preferred_locale.sql
--
-- Language tag per user so the welcome email (and any future transactional
-- email) can be sent in the right language. Until now every user received the
-- Spanish welcome template; with the Hong Kong launch we now get English users
-- and need to branch.
--
-- Stores the raw locale tag (e.g. 'es-MX', 'en', 'en-US', 'zh-HK'); the backend
-- derives a 2-way es/en language from it at send time. Go-forward, new users
-- get their tag from the browser Accept-Language header at first login; this
-- migration backfills existing users from their observed analytics locale.
--
-- Additive + idempotent. The new column is nullable and defaults NULL; the
-- backfill UPDATEs only touch rows where preferred_locale IS NULL, so this is
-- safe to run on production and safe to re-run (a second run is a no-op).
--
-- Apply: paste into the Supabase SQL editor and run (or your normal migration
-- runner). No view rebuild required.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS preferred_locale text;

-- ── Backfill: observed browser locale from analytics_events ──────────────────
-- analytics_events.wallet_address (lowercase) maps to users.primary_address /
-- users.eoa_address. Use the most common locale we've ever seen for that wallet.
-- Primary address wins; fall back to the EOA address for anything still null.

WITH loc AS (
  SELECT lower(wallet_address) AS w,
         mode() WITHIN GROUP (ORDER BY locale) AS locale_mode
  FROM public.analytics_events
  WHERE wallet_address IS NOT NULL
    AND locale IS NOT NULL
    AND locale <> ''
  GROUP BY 1
)
UPDATE public.users u
SET preferred_locale = l.locale_mode
FROM loc l
WHERE u.preferred_locale IS NULL
  AND u.primary_address IS NOT NULL
  AND l.w = lower(u.primary_address);

WITH loc AS (
  SELECT lower(wallet_address) AS w,
         mode() WITHIN GROUP (ORDER BY locale) AS locale_mode
  FROM public.analytics_events
  WHERE wallet_address IS NOT NULL
    AND locale IS NOT NULL
    AND locale <> ''
  GROUP BY 1
)
UPDATE public.users u
SET preferred_locale = l.locale_mode
FROM loc l
WHERE u.preferred_locale IS NULL
  AND u.eoa_address IS NOT NULL
  AND l.w = lower(u.eoa_address);

-- Everyone with no analytics signal predates the HK launch and is Spanish
-- audience — default them to 'es' (matches the previous unconditional behavior).
UPDATE public.users
SET preferred_locale = 'es'
WHERE preferred_locale IS NULL;
