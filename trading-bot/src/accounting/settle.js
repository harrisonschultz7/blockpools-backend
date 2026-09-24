// trading-bot/src/accounting/settle.js
//
// Settlement + CLV grading.
//
// CLV (closing line value) is THE metric for this bot, not P&L. With ~317
// games of usable history and a bot that deliberately trades a handful per
// week, P&L is almost pure variance for a long time -- a 55% true edge and a
// 45% true edge look identical over 30 bets. CLV converges far faster, because
// every trade produces a graded observation regardless of whether the game was
// won or lost: did the price move our way by kickoff?
//
// A bot with positive CLV and negative P&L is unlucky. A bot with negative CLV
// and positive P&L is lucky, and will give it back. Grade on CLV.

const { q } = require("../db");
const log = require("../log");

/**
 * Freeze the closing price for every trade whose game has kicked off.
 * Closing price = the last book mid recorded at or before kickoff.
 */
async function gradeClv() {
  const { rows } = await q(
    `with closing as (
       select t.id trade_id,
              (select o.mid from sports.odds_history o
                where o.token_id = t.token_id
                  and o.ts <= g.kickoff
                order by o.ts desc limit 1) close_mid
         from bots.trades t
         join sports.nfl_games g on g.game_id = t.game_id
        where t.closing_price is null and g.kickoff <= now()
     )
     update bots.trades t
        set closing_price = c.close_mid,
            clv_bps = ((c.close_mid - t.fill_price) / nullif(t.fill_price,0)) * 10000
       from closing c
      where t.id = c.trade_id and c.close_mid is not null
      returning t.id, t.clv_bps`,
  );
  if (rows.length) {
    const avg = rows.reduce((s, r) => s + Number(r.clv_bps), 0) / rows.length;
    log(`clv: graded ${rows.length} trades, mean ${avg.toFixed(0)} bps`);
  }
  return rows.length;
}

/**
 * Settle trades whose game is final.
 * A share pays $1 if its side won, $0 otherwise -- so pnl = payout - cost.
 * Positions closed early by a resting exit are excluded: their P&L was booked
 * at the sale, and settling them again would inflate the record with shares
 * the bot no longer held.
 */
async function settleTrades() {
  const { rows } = await q(
    `update bots.trades t
        set settled = true,
            closed_at = now(),
            won = w.won,
            pnl_usd = case when w.won then t.shares - t.notional_usd
                           else -t.notional_usd end
       from (
         select t2.id,
                (case when t2.side = 'home' then g.home_score > g.away_score
                      else g.away_score > g.home_score end) won
           from bots.trades t2
           join sports.nfl_games g on g.game_id = t2.game_id
          where t2.settled = false
            and t2.exited = false   -- closed by a limit sell; paying it out again
                                    -- would double-count the same shares
            and g.home_score is not null and g.away_score is not null
            and g.home_score <> g.away_score
       ) w
      where t.id = w.id
      returning t.id, t.won, t.pnl_usd`,
  );

  if (rows.length) {
    // Clear the settled positions so NAV stops marking them.
    await q(
      `delete from bots.positions p
        where not exists (
          select 1 from bots.trades t
           where t.bot_id = p.bot_id and t.token_id = p.token_id and t.settled = false)`,
    );
    const pnl = rows.reduce((s, r) => s + Number(r.pnl_usd), 0);
    const wins = rows.filter((r) => r.won).length;
    log(`settled ${rows.length} trades: ${wins}W-${rows.length - wins}L, pnl $${pnl.toFixed(2)}`);
  }
  return rows.length;
}

/** Headline record for the homepage card, computed from NAV and CLV. */
async function botSummary(botId) {
  const { rows } = await q(
    `select
       count(*) filter (where settled) settled_trades,
       count(*) filter (where settled and won) wins,
       count(*) filter (where settled and not won) losses,
       coalesce(sum(pnl_usd) filter (where settled), 0) realized_pnl,
       avg(clv_bps) filter (where clv_bps is not null) mean_clv_bps,
       count(*) filter (where clv_bps > 0) clv_positive,
       count(*) filter (where clv_bps is not null) clv_graded
     from bots.trades where bot_id = $1`, [botId]);
  const nav = await q(
    `select nav_usd, d from bots.nav_history where bot_id = $1 order by d desc limit 1`, [botId]);
  const start = await q(`select starting_nav from bots.bot where id = $1`, [botId]);

  const s = rows[0];
  const navNow = nav.rows[0] ? Number(nav.rows[0].nav_usd) : Number(start.rows[0].starting_nav);
  const startNav = Number(start.rows[0].starting_nav);
  return {
    ...s,
    nav: navNow,
    roi_pct: ((navNow - startNav) / startNav) * 100,
  };
}

module.exports = { gradeClv, settleTrades, botSummary };
