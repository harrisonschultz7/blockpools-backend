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
 *
 * TOTALS NEED NO PUSH BRANCH. Verified against all 2028 open NFL totals markets
 * on 2026-09-26: every line is a half-point (44.5, 46.5, ...) and not one was an
 * integer, so a total can never land exactly on the line. If Polymarket ever
 * lists an integer line, this comparison silently grades a push as an under and
 * the resolution text ("45 or more points") is where the real rule lives.
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
                -- BRANCHED ON MARKET TYPE. The moneyline form alone was actively
                -- wrong for Argo-7: side is 'over' or 'under', so "side = 'home'"
                -- is false and every totals trade fell through to
                -- "away_score > home_score" -- settled as an away moneyline bet,
                -- with a plausible-looking win rate and no error anywhere.
                (case
                   when t2.market_type = 'totals' then
                     case when t2.side = 'over'
                          then (g.home_score + g.away_score) > t2.line
                          else (g.home_score + g.away_score) < t2.line end
                   when t2.side = 'home' then g.home_score > g.away_score
                   else g.away_score > g.home_score
                 end) won
           from bots.trades t2
           join sports.nfl_games g on g.game_id = t2.game_id
          where t2.settled = false
            and t2.exited = false   -- closed by a limit sell; paying it out again
                                    -- would double-count the same shares
            and g.home_score is not null and g.away_score is not null
            -- A tie voids a moneyline but is a perfectly good total, so the
            -- draw exclusion applies only to sides. Totals need t2.line instead:
            -- without it the comparison above is against NULL and settles nothing.
            and (t2.market_type = 'totals'
                 or g.home_score <> g.away_score)
            and (t2.market_type <> 'totals' or t2.line is not null)
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

/**
 * Book one exchange-resolved trade: settle it, retire its resting exit, and mark
 * the market closed. Returns 1 if it settled, 0 if it was left alone.
 */
async function applyResolution(t, won) {
  // CROSS-CHECK against the box score when we happen to have one. The two should
  // never disagree; if they do, the safe move is to settle nothing and say so,
  // because one of the two feeds is wrong and guessing which would be a coin flip
  // applied to real ledger rows.
  if (t.home_score !== null && t.away_score !== null && t.line !== null) {
    const total = Number(t.home_score) + Number(t.away_score);
    const byScore = t.side === "over" ? total > Number(t.line) : total < Number(t.line);
    if (byScore !== won) {
      log.err(
        `SETTLEMENT MISMATCH ${t.game_id} ${t.side} ${t.line}: exchange says ` +
        `${won ? "won" : "lost"}, box score (${total}) says ` +
        `${byScore ? "won" : "lost"} -- left unsettled for review`);
      return 0;
    }
  }

  const { rows } = await q(
    `update bots.trades
        set settled = true, won = $2, closed_at = now(),
            pnl_usd = case when $2 then shares - notional_usd
                           else -notional_usd end
      where id = $1 and settled = false
      returning pnl_usd`,
    [t.id, won],
  );
  if (!rows.length) return 0;   // something else settled it first

  // The resting sell has nothing left to sell into.
  await q(
    `update bots.limit_orders
        set status = 'cancelled', closed_reason = 'market_resolved', updated_at = now()
      where trade_id = $1 and status = 'open'`,
    [t.id],
  );

  // MARK THE MARKET CLOSED. Nothing else ever does: the markets ingest queries
  // Gamma with closed=false, so a market that closes simply drops out of the feed
  // and its row keeps closed=false forever. That left the recorder polling a dead
  // token every 20s for 404s, and made the market_closed guard in manageExits
  // unreachable. This only retires markets we actually held.
  await q(
    `update sports.pm_totals_markets set closed = true where condition_id = $1`,
    [t.condition_id],
  );

  log(`    RESOLVED ${t.game_id} ${t.side} ${t.line} -> ` +
      `${won ? "WON" : "LOST"} pnl ${Number(rows[0].pnl_usd).toFixed(2)}`);
  return 1;
}
/** Resolution as the exchange itself reports it. null = not resolved yet. */
async function marketResolution(conditionId) {
  const res = await fetch(`https://clob.polymarket.com/markets/${conditionId}`);
  if (!res.ok) return null;
  const m = await res.json();
  if (!m || !m.closed || !Array.isArray(m.tokens)) return null;
  // `closed` alone is not resolution -- a market can close before the oracle
  // reports. A winner must actually be named.
  if (!m.tokens.some((t) => t.winner === true)) return null;
  return m.tokens;
}

/**
 * Settle from the EXCHANGE'S resolution rather than from a box score.
 *
 * settleTrades() waits on sports.nfl_games.home_score, which comes from nflverse
 * and lags the final whistle by hours. Polymarket resolves far sooner, and once it
 * has, the position is worth exactly 0 or 1 and nothing about it is open any more.
 * CIN @ PIT sat showing as an open position with a live mark long after the market
 * had paid out and the token had stopped existing -- the book 404s, so even the
 * mark was stale. A resolved loss should read as a loss.
 *
 * Matched on token_id, never on the outcome string. Our token either won or it did
 * not; parsing "Over"/"Under" back into a side would reintroduce exactly the kind
 * of string-matching that mis-settled a shared-mascot market once already.
 */
async function settleFromMarkets() {
  const { rows: pending } = await q(
    `select t.id, t.bot_id, t.condition_id, t.token_id, t.shares, t.notional_usd,
            t.game_id, t.side, t.line, g.kickoff, g.home_score, g.away_score
       from bots.trades t
       join sports.nfl_games g on g.game_id = t.game_id
      where t.settled = false
        and t.exited = false
        and t.condition_id is not null
        -- KICKOFF + 2h FLOOR. A market reporting a winner before the game can
        -- plausibly have finished is a bad feed, not a result: a stale settler
        -- once resolved four pre-kickoff markets against the previous day's
        -- scores. Anything earlier than this is reported and left alone.
        and g.kickoff < now() - interval '2 hours'`,
  );
  if (!pending.length) return 0;

  // One fetch per market, not per trade.
  const byCondition = new Map();
  for (const t of pending) {
    if (!byCondition.has(t.condition_id)) byCondition.set(t.condition_id, []);
    byCondition.get(t.condition_id).push(t);
  }

  let settled = 0;
  for (const [conditionId, trades] of byCondition) {
    let tokens;
    try { tokens = await marketResolution(conditionId); }
    catch (e) { log.warn(`resolution fetch failed ${conditionId}: ${e.message}`); continue; }
    if (!tokens) continue;

    for (const t of trades) {
      const tok = tokens.find((x) => String(x.token_id) === String(t.token_id));
      if (!tok) {
        log.warn(`resolved market ${conditionId} has no token ${t.token_id}; left open`);
        continue;
      }
      const won = tok.winner === true;
      settled += await applyResolution(t, won);
    }
  }

  if (settled) {
    await q(
      `delete from bots.positions p
        where not exists (
          select 1 from bots.trades t
           where t.bot_id = p.bot_id and t.token_id = p.token_id
             and t.settled = false)`,
    );
  }
  return settled;
}
/** Headline record for the homepage card, computed from NAV and CLV. */
async function botSummary(botId) {
  const { rows } = await q(
    `select
       count(*) filter (where settled and pnl_usd is not null) settled_trades,
       -- Profitable trades, not game outcomes: a position closed early at fair
       -- value is a win whoever wins the game. Must match src/routes/bots.ts.
       count(*) filter (where settled and pnl_usd > 0) wins,
       count(*) filter (where settled and pnl_usd <= 0) losses,
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

module.exports = { gradeClv, settleTrades, settleFromMarkets, botSummary };
