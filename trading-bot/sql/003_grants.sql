-- trading-bot/sql/003_grants.sql
--
-- Server-only from day one. These schemas hold proprietary model output --
-- factor weights, p_fair, and the bot's open positions. Anyone able to read
-- sports.features before kickoff can front-run the bot; anyone able to read
-- bots.positions knows exactly what it holds.
--
-- Consistent with the 2026-08 lockdown: REVOKE rather than rely on RLS, since
-- the backend connects with the service role and never needs the anon path.

revoke all on schema sports from anon, authenticated;
revoke all on schema bots   from anon, authenticated;

revoke all on all tables    in schema sports from anon, authenticated;
revoke all on all sequences in schema sports from anon, authenticated;
revoke all on all tables    in schema bots   from anon, authenticated;
revoke all on all sequences in schema bots   from anon, authenticated;

-- Future tables in these schemas inherit the same denial.
alter default privileges in schema sports revoke all on tables    from anon, authenticated;
alter default privileges in schema sports revoke all on sequences from anon, authenticated;
alter default privileges in schema bots   revoke all on tables    from anon, authenticated;
alter default privileges in schema bots   revoke all on sequences from anon, authenticated;

-- NOTE: the public bot track record (NAV curve, record, ROI) must therefore be
-- served through a backend endpoint that reads with the service role, NOT by
-- pointing the frontend Supabase client at bots.nav_history.
