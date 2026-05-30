-- src/db/migrations/2026-05-30_analytics_events.sql
--
-- Lightweight first-party click & navigation analytics.
--
-- One row per tracked interaction:
--   * event_type='click'      -> a tagged button was clicked (name = button id)
--   * event_type='page_view'  -> a page visit closed out (name/page_path = path,
--                                duration_ms = ms spent on that page)
--
-- Rows are inserted ONLY by the backend service-role client
-- (POST /api/analytics/track), so no RLS insert policy is required.
-- wallet_address is stored lowercased to join cleanly against
-- user_trade_events and other wallet-keyed tables. It is null until the
-- user connects a wallet — those rows are attributable by session_id only.
--
-- Idempotent — safe to paste into the Supabase SQL editor and re-run.

CREATE TABLE IF NOT EXISTS public.analytics_events (
  id             bigint generated always as identity primary key,
  created_at     timestamptz not null default now(),
  session_id     text not null,
  wallet_address text,                 -- null = wallet not connected
  event_type     text not null,        -- 'click' | 'page_view'
  name           text not null,        -- button id, or path for page_view
  page_path      text not null,
  duration_ms    integer,              -- set on page_view when leaving the page
  metadata       jsonb
);

-- "all events for a given user" — profile/funnel joins against user_trade_events.
CREATE INDEX IF NOT EXISTS analytics_events_wallet_idx
  ON public.analytics_events (wallet_address);

-- "how many times was button X clicked / page X viewed" — top aggregation query.
CREATE INDEX IF NOT EXISTS analytics_events_type_name_idx
  ON public.analytics_events (event_type, name);

-- time-window scans (last 24h, last 7d dashboards).
CREATE INDEX IF NOT EXISTS analytics_events_created_at_idx
  ON public.analytics_events (created_at);
