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
      `select id, name, league, risk_tier, mode, enabled, starting_nav, created_at,
              market_scope, description
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
           -- RECORD = PROFITABLE TRADES, not game outcomes. The bot can close a
           -- position before kickoff by filling its resting sell at fair value, in
           -- which case who wins the game is irrelevant -- it already banked the
           -- move. Scoring on "won" was wrong twice over: it mislabelled those
           -- trades, and because settleExit never sets "won" at all, a limit-exited
           -- trade counted as NEITHER a win nor a loss and vanished from the record.
           count(*) filter (where settled and pnl_usd is not null) as settled_trades,
           count(*) filter (where settled and pnl_usd > 0) as wins,
           count(*) filter (where settled and pnl_usd <= 0) as losses,
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
      //
      // COUNT DISTINCT GAMES, not decision rows. Every tick writes one row per
      // game it looks at, so a plain count(*) counts EVALUATIONS: three manual
      // runs over 16 games already read as 49. Under the 15-minute timer it
      // would reach ~1,536/day and the tile would show "2/1536", which looks
      // like extraordinary selectivity but is just the tick rate.
      const { rows: selRows } = await pg.query(
        `select count(distinct game_id) as looks,
                count(distinct game_id) filter (where acted) as acted
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
        marketScope: b.market_scope,
        description: b.description,
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

/**
 * Trade-by-trade event log.
 *
 * One bots.trades row is a POSITION, but the page needs EVENTS: a buy, then a
 * sell if it closed. So each row fans out into one or two events, and only the
 * closing event carries a return -- a buy has no return yet, by definition.
 *
 * A position closes one of two ways, and both are reported honestly:
 *   - exit_price set  -> the resting sell filled at fair value before kickoff
 *   - settled         -> the game decided it; the share paid 1.00 or 0.00
 *
 * Only games that have started are exposed. An open pre-kickoff position would
 * reveal what the model likes while the market is still trading it.
 */
botsRouter.get("/:botId/trades", async (req, res) => {
  try {
    const limit = Math.min(200, Number(req.query.limit) || 50);
    const { rows } = await pg.query(
      `select t.id, t.game_id, t.side, t.settled, t.won, t.exited,
              t.fill_price, t.shares, t.notional_usd,
              t.exit_price, t.exit_at, t.exit_shares, t.exit_reason,
              t.pnl_usd, t.clv_bps, t.opened_at, t.closed_at,
              g.away_team, g.home_team, g.kickoff, g.week, g.season
         from bots.trades t
         join sports.nfl_games g on g.game_id = t.game_id
        where t.bot_id = $1 and g.kickoff <= now()
        order by t.opened_at desc
        limit $2`,
      [req.params.botId, limit],
    );

    const events: any[] = [];
    for (const t of rows) {
      const matchup = `${t.away_team} @ ${t.home_team}`;
      const team = t.side === "home" ? t.home_team : t.away_team;
      const base = {
        tradeId: t.id, gameId: t.game_id, matchup, team, side: t.side,
        week: t.week, season: t.season, kickoff: t.kickoff,
      };

      events.push({
        ...base,
        kind: "BUY",
        ts: t.opened_at,
        price: Number(t.fill_price),
        shares: Number(t.shares),
        usd: Number(t.notional_usd),
        // A buy has no return yet. Explicit null so the UI never prints 0.00%.
        returnUsd: null,
        returnPct: null,
        clvBps: t.clv_bps === null ? null : Number(t.clv_bps),
      });

      if (t.exit_price !== null && t.exit_price !== undefined) {
        const shares = Number(t.exit_shares || t.shares);
        const px = Number(t.exit_price);
        const cost = Number(t.fill_price);
        events.push({
          ...base,
          kind: "SELL",
          ts: t.exit_at,
          price: px,
          shares,
          usd: shares * px,
          returnUsd: shares * (px - cost),
          returnPct: cost > 0 ? ((px - cost) / cost) * 100 : null,
          reason: t.exit_reason || "sold",
        });
      } else if (t.settled) {
        // Settlement is a sale at 1.00 (won) or 0.00 (lost).
        const px = t.won ? 1 : 0;
        const cost = Number(t.fill_price);
        events.push({
          ...base,
          kind: "SELL",
          ts: t.closed_at,
          price: px,
          shares: Number(t.shares),
          usd: Number(t.shares) * px,
          returnUsd: Number(t.pnl_usd),
          returnPct: cost > 0 ? ((px - cost) / cost) * 100 : null,
          reason: t.won ? "settled_win" : "settled_loss",
        });
      }
    }

    events.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());
    res.json({ events });
  } catch (e: any) {
    console.error("[bots] trades failed:", e?.message || e);
    res.status(500).json({ error: "bots_trades_unavailable" });
  }
});


export default botsRouter;
