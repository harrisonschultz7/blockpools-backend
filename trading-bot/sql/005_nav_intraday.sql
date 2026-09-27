-- trading-bot/sql/005_nav_intraday.sql
--
-- Intraday NAV points, so a model's curve has shape within a day.
--
-- bots.nav_history is keyed (bot_id, d) and is deliberately DAILY -- it is what
-- "best day" and the long-run track record read, and one authoritative close per
-- day is the right grain for that. It cannot also hold intraday points without
-- either changing its key or overwriting itself all day, so the intraday series
-- lives here and the daily one is left alone.
--
-- COST, measured before building it: 2 bots on a 15-minute tick is 192 rows and
-- ~21 KB a day, 7 MB a year unbounded, and 0.3 MB at steady state under the
-- 14-day retention below. For scale, the depth recorder was writing 195,662 rows
-- and ~250 MB in a single day before this session's fixes -- a whole year of this
-- table costs about 0.03 days of that.
--
-- Retention matters anyway, and not for the bytes: an unbounded intraday series
-- would eventually make the chart query scan years of points to draw two weeks.
-- run/daily.js prunes past navIntradayRetentionDays; the daily series already
-- covers everything older.
create table if not exists bots.nav_intraday (
  bot_id             text not null references bots.bot (id),
  ts                 timestamptz not null,
  nav_usd            numeric not null,
  cash_usd           numeric not null,
  position_value_usd numeric not null default 0,
  open_positions     int not null default 0,
  primary key (bot_id, ts)
);

-- The only read pattern: one bot's recent points, oldest first.
create index if not exists bots_nav_intraday_idx
  on bots.nav_intraday (bot_id, ts desc);

revoke all on bots.nav_intraday from anon, authenticated;
