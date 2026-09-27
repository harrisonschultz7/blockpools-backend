// trading-bot/src/accounting/nav.js
//
// NAV IS THE PRODUCT. The homepage ROI and performance chart read
// bots.nav_history -- never a sum of realised trades.
//
// Realised-only math is what produced the 1,098%-style ROI numbers on the user
// leaderboard: closed winners count, open losers are invisible, so the curve
// only ever goes up and the number is meaningless. Here every open position is
// marked to the last recorded book mid, so an unrealised loss lands on the day
// it happens and the curve can fall.

const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");

/** Mark open positions to the most recent recorded book. */
async function markPositions(botId) {
  const { rows } = await q(
    `select p.token_id, p.game_id, p.side, p.shares, p.cost_usd,
            (select o.mid from sports.odds_history o
              where o.token_id = p.token_id order by o.ts desc limit 1) mark
       from bots.positions p
      where p.bot_id = $1 and p.shares > 0`,
    [botId],
  );
  let value = 0;
  let cost = 0;
  for (const r of rows) {
    // No recent book (market closed, or never recorded) -> fall back to cost,
    // which is neutral rather than optimistic.
    const mark = r.mark !== null && r.mark !== undefined
      ? Number(r.mark)
      : Number(r.cost_usd) / Number(r.shares);
    value += Number(r.shares) * mark;
    cost += Number(r.cost_usd);
  }
  return { positionValue: value, positionCost: cost, openPositions: rows.length };
}

/**
 * Seed the curve's first point: the bot's LAUNCH.
 *
 * nav_history only gains a row when the daily job runs, so a bot registered
 * between runs has exactly one point and the chart correctly refuses to draw a
 * line through it -- which is what "Not enough history to chart yet" was on
 * Argo-7's page while it held seven live positions.
 *
 * The launch row is not a placeholder: the bot really did start at starting_nav
 * with nothing open, on the day it was created. Writing it means the curve
 * starts where the money started rather than at whenever the first snapshot
 * happened to land.
 *
 * ON CONFLICT DO NOTHING so a bot created and first snapshotted on the same day
 * keeps the real snapshot, not a synthetic flat one.
 */
async function ensureLaunchRow(botId) {
  const { rowCount } = await q(
    `insert into bots.nav_history
       (bot_id, d, nav_usd, cash_usd, position_value_usd,
        realized_pnl_usd, unrealized_pnl_usd, open_positions)
     select b.id, b.created_at::date, b.starting_nav, b.starting_nav, 0, 0, 0, 0
       from bots.bot b
      where b.id = $1
     on conflict (bot_id, d) do nothing`,
    [botId],
  );
  if (rowCount) log(`nav: seeded launch row for ${botId}`);
  return rowCount;
}

/** Write today's NAV row. Idempotent -- re-running the same day overwrites. */
async function snapshotNav(botId, day) {
  const c = cfg();
  const d = day || new Date().toISOString().slice(0, 10);

  // Before anything else, so a brand-new bot has two points the first time this
  // runs rather than one.
  await ensureLaunchRow(botId);

  const bot = await q(`select starting_nav from bots.bot where id = $1`, [botId]);
  const startingNav = bot.rows[0] ? Number(bot.rows[0].starting_nav) : c.paper.startingNavUsd;

  // Cash = starting capital, minus what open positions cost, plus what settled
  // ones returned. Settled trades carry their own pnl.
  const spent = await q(
    `select coalesce(sum(cost_usd), 0) v from bots.positions where bot_id = $1 and shares > 0`,
    [botId]);
  const realized = await q(
    `select coalesce(sum(pnl_usd), 0) v from bots.trades where bot_id = $1 and settled = true`,
    [botId]);

  const { positionValue, positionCost, openPositions } = await markPositions(botId);
  const cash = startingNav - Number(spent.rows[0].v) + Number(realized.rows[0].v);
  const nav = cash + positionValue;

  await q(
    `insert into bots.nav_history
       (bot_id, d, nav_usd, cash_usd, position_value_usd,
        realized_pnl_usd, unrealized_pnl_usd, open_positions)
     values ($1,$2,$3,$4,$5,$6,$7,$8)
     on conflict (bot_id, d) do update set
       nav_usd = excluded.nav_usd, cash_usd = excluded.cash_usd,
       position_value_usd = excluded.position_value_usd,
       realized_pnl_usd = excluded.realized_pnl_usd,
       unrealized_pnl_usd = excluded.unrealized_pnl_usd,
       open_positions = excluded.open_positions, computed_at = now()`,
    [botId, d, nav, cash, positionValue,
     Number(realized.rows[0].v), positionValue - positionCost, openPositions],
  );

  log(`nav ${botId} ${d}: $${nav.toFixed(2)} (cash ${cash.toFixed(2)} + positions ${positionValue.toFixed(2)}, ${openPositions} open)`);
  return nav;
}

/** Current NAV, for position sizing. Falls back to the configured start. */
async function currentNav(botId) {
  const { rows } = await q(
    `select nav_usd from bots.nav_history where bot_id = $1 order by d desc limit 1`,
    [botId]);
  if (rows[0]) return Number(rows[0].nav_usd);
  const bot = await q(`select starting_nav from bots.bot where id = $1`, [botId]);
  return bot.rows[0] ? Number(bot.rows[0].starting_nav) : cfg().paper.startingNavUsd;
}

module.exports = {
  ensureLaunchRow, snapshotNav, currentNav, markPositions };
