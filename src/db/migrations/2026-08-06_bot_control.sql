-- ============================================================================
-- Bot control: a simple runtime on/off flag for background bots (starting with
-- the live order-book seeding bot, seed-bot.js).
--
-- Run this in the Supabase SQL editor. It is idempotent (safe to re-run).
--
-- Why: the seed bot's systemd service always runs, but it only SEEDS while its
-- flag here is enabled. This gives a monitorable start/stop switch you can flip
-- from the Supabase dashboard, the admin endpoint
-- (POST /api/v2/seed-bot/control), or an admin UI button — without SSHing in to
-- stop the service. When the flag is off (or unreadable) the bot cancels its
-- resting orders and idles. `systemctl stop` remains the hard kill.
--
-- One row per bot_name. Reads are public (non-sensitive); writes go through the
-- V2_MATCHER_SECRET-guarded endpoint.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.bot_control (
  bot_name   text        PRIMARY KEY,
  enabled    boolean     NOT NULL DEFAULT false,
  note       text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Seed the row for the live seeding bot, OFF by default (opt-in to seeding).
INSERT INTO public.bot_control (bot_name, enabled, note)
VALUES ('seed-bot', false, 'Live Polymarket-priced order-book seeding bot')
ON CONFLICT (bot_name) DO NOTHING;
