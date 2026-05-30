-- src/db/migrations/2026-05-30_analytics_events_enrich.sql
--
-- Enriches analytics_events with dimensions the dashboard segments on.
-- All nullable + IF NOT EXISTS — safe to re-run, existing rows stay valid.
--
--   occurred_at : real client event time (epoch ms -> timestamptz). created_at
--                 is the server batch-insert time and collapses ordering/timing
--                 within a flush; occurred_at preserves the true sequence.
--   locale      : active UI language at event time ("en", "es", ...).
--   device      : coarse viewport class ("mobile" | "tablet" | "desktop").
--
-- Apply: paste into the Supabase SQL editor and run.

ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS occurred_at timestamptz;

ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS locale text;

ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS device text;

-- Ordering events within a session by true client time is the core operation
-- behind the navigation flow / Sankey. Index the access pattern.
CREATE INDEX IF NOT EXISTS analytics_events_session_occurred_idx
  ON public.analytics_events (session_id, occurred_at);
