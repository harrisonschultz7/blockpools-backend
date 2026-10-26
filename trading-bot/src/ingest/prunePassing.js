// trading-bot/src/ingest/prunePassing.js
//
// Retention for passing-prop order books (Bruno-7). Called from run/daily.js,
// whose blanket finished-game prune now skips prop tokens and hands them here.

const { cfg } = require("../config");
const { q } = require("../db");

/**
 * Bounded retention for FINISHED games' passing-prop books.
 *
 *   1. unused  -- lines no forecast or trade ever touched (the recorder keeps 3
 *                 per QB; Bruno-7 forecasts one) are deleted outright.
 *   2. thinned -- the kept lines are cut to one row per token per 10 minutes,
 *                 EXCEPT from 3h before kickoff onward, which stays at full
 *                 resolution: that is the closing-line and in-play window the
 *                 depth/fill question is about.
 *   3. expired -- everything older than passingBookRetentionDays after kickoff.
 *
 * Measured 2026-10-26: one game's 6 prop tokens wrote ~4,600 rows (~575 bytes
 * each) in 26h, so unbounded retention would add ~40 MB a week. This keeps a
 * week to a few MB.
 */
async function prunePassingBooks() {
  const days = Number(cfg().ingest.passingBookRetentionDays) || 35;
  const finishedProps = `
    select o.id, o.token_id, o.ts, g.kickoff
      from sports.odds_history o
      join sports.nfl_games g on g.game_id = o.game_id
      join sports.pm_passing_markets m
        on o.token_id in (m.over_token_id, m.under_token_id)
     where g.home_score is not null`;

  const unused = await q(
    `delete from sports.odds_history h using (${finishedProps}) x
      join sports.pm_passing_markets m on x.token_id in (m.over_token_id, m.under_token_id)
     where h.id = x.id
       and not exists (select 1 from sports.features_passing f where f.condition_id = m.condition_id)
       and not exists (select 1 from bots.trades t where t.condition_id = m.condition_id)`,
  );
  const thinned = await q(
    `delete from sports.odds_history h using (
       select id from (
         select x.id, x.ts, x.kickoff,
                row_number() over (
                  partition by x.token_id,
                               date_trunc('hour', x.ts),
                               floor(extract(minute from x.ts) / 10)
                  order by x.ts desc) as rn
           from (${finishedProps}) x
       ) r
      where r.rn > 1 and r.ts < r.kickoff - interval '3 hours'
     ) d
     where h.id = d.id`,
  );
  const expired = await q(
    `delete from sports.odds_history h using (${finishedProps}) x
     where h.id = x.id and x.kickoff < now() - ($1 || ' days')::interval`,
    [String(days)],
  );
  return { unused: unused.rowCount, thinned: thinned.rowCount, expired: expired.rowCount };
}

module.exports = { prunePassingBooks };
