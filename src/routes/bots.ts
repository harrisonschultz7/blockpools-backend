// src/routes/bots.ts
//
// Public read surface for the trading bots (Astro).
//
// The `bots` and `sports` schemas are REVOKED from anon/authenticated on
// purpose: sports.features holds p_fair before kickoff, and bots.positions
// holds live exposure. Anyone able to read either could front-run the bot. So
// the frontend cannot use the Supabase client here -- everything goes through
// this endpoint, which reads with the service-role pool and returns only what
// is safe to publish.
//
// Deliberately NOT exposed: p_fair for any un-started game, factor weights,
// resting limit prices, or open position sizes for upcoming games.

import { Router } from "express";
import { pg } from "../db/pg";

const botsRouter = Router();

const CACHE_MS = 60_000;
let cache: { at: number; body: any } | null = null;

botsRouter.get("/", async (_req, res) => {
  try {
    if (cache && Date.now() - cache.at < CACHE_MS) return res.json(cache.body);

    const { rows: bots } = await pg.query(
      `select id, name, league, risk_tier, mode, enabled, starting_nav, created_at
         from bots.bot where enabled = true order by created_at`,
    );

    const out = [];
    for (const b of bots) {
      // Record and ROI. Only SETTLED trades count toward the record; NAV is the
      // source of truth for ROI so open losses are visible (a realised-only
      // number can only ever go up, which is how the 1,098% ROI figures on the
      // user leaderboard happened).
      const { rows: recRows } = await pg.query(
        `select
           count(*) filter (where settled) as settled_trades,
           count(*) filter (where settled and won) as wins,
           count(*) filter (where settled and won = false) as losses,
           count(*) filter (where settled = false) as open_trades,
           coalesce(sum(pnl_usd) filter (where settled), 0) as realized_pnl,
           avg(clv_bps) filter (where clv_bps is not null) as mean_clv_bps,
           count(*) filter (where clv_bps is not null) as clv_graded,
           count(*) filter (where clv_bps > 0) as clv_positive
         from bots.trades where bot_id = $1`,
        [b.id],
      );
      const rec = recRows[0];

      const { rows: navRows } = await pg.query(
        `select d, nav_usd, open_positions
           from bots.nav_history where bot_id = $1 order by d`,
        [b.id],
      );

      // Selectivity is part of the story: this bot is meant to pass on most
      // games, so "traded 2 of 16" belongs on the card.
      const { rows: selRows } = await pg.query(
        `select count(*) as looks, count(*) filter (where acted) as acted
           from bots.decisions
          where bot_id = $1 and asof_ts > now() - interval '8 days'`,
        [b.id],
      );

      const startNav = Number(b.starting_nav);
      const nav = navRows.length ? Number(navRows[navRows.length - 1].nav_usd) : startNav;

      out.push({
        id: b.id,
        name: b.name,
        league: b.league,
        riskTier: b.risk_tier,
        mode: b.mode,                  // 'paper' — the UI must label this
        startingNav: startNav,
        nav,
        roiPct: startNav > 0 ? ((nav - startNav) / startNav) * 100 : 0,
        record: {
          wins: Number(rec.wins),
          losses: Number(rec.losses),
          settledTrades: Number(rec.settled_trades),
          openTrades: Number(rec.open_trades),
          realizedPnl: Number(rec.realized_pnl),
        },
        clv: {
          meanBps: rec.mean_clv_bps === null ? null : Number(rec.mean_clv_bps),
          graded: Number(rec.clv_graded),
          positive: Number(rec.clv_positive),
        },
        selectivity: {
          looks: Number(selRows[0].looks),
          acted: Number(selRows[0].acted),
        },
        navSeries: navRows.map((r: any) => ({
          d: r.d instanceof Date ? r.d.toISOString().slice(0, 10) : String(r.d).slice(0, 10),
          nav: Number(r.nav_usd),
        })),
        createdAt: b.created_at,
      });
    }

    const body = { bots: out };
    cache = { at: Date.now(), body };
    res.json(body);
  } catch (e: any) {
    console.error("[bots] failed:", e?.message || e);
    res.status(500).json({ error: "bots_unavailable" });
  }
});

/** Settled trade history for a bot — safe to publish, the games are over. */
botsRouter.get("/:botId/trades", async (req, res) => {
  try {
    const limit = Math.min(100, Number(req.query.limit) || 25);
    const { rows } = await pg.query(
      `select t.game_id, t.side, t.settled, t.won,
              t.fill_price, t.exit_price, t.exit_reason, t.shares,
              t.notional_usd, t.pnl_usd, t.clv_bps, t.opened_at, t.closed_at,
              g.away_team, g.home_team, g.kickoff, g.week, g.season
         from bots.trades t
         join sports.nfl_games g on g.game_id = t.game_id
        where t.bot_id = $1 and g.kickoff <= now()
        order by t.opened_at desc limit $2`,
      [req.params.botId, limit],
    );
    res.json({ trades: rows });
  } catch (e: any) {
    console.error("[bots] trades failed:", e?.message || e);
    res.status(500).json({ error: "bots_trades_unavailable" });
  }
});

export default botsRouter;
