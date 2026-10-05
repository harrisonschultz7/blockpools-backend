// src/routes/bots.ts
//
// Public read surface for the trading bots (Adam-7).
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

      // Intraday points for the recent window. The daily series carries the long
      // run; these give the last fortnight shape, so a position moving all
      // afternoon is visible instead of appearing as one step at midnight.
      const { rows: intraRows } = await pg.query(
        `select ts, nav_usd, open_positions
           from bots.nav_intraday
          where bot_id = $1 and ts > now() - interval '14 days'
          order by ts`,
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
      // WHEN THIS BOT WAS ACTUALLY EXPOSED. One window per game it traded,
      // opening two hours before kickoff -- roughly when it takes positions -- and
      // closing when the last of that game's trades closed, or four hours after
      // kickoff if none has. Used to strip dead time out of the equity curve; see
      // buildNavSeries().
      const { rows: windowRows } = await pg.query(
        `select min(g.kickoff) - interval '2 hours' as w_start,
                coalesce(max(t.closed_at), max(t.exit_at),
                         min(g.kickoff) + interval '4 hours') as w_end
           from bots.trades t
           join sports.nfl_games g on g.game_id = t.game_id
          where t.bot_id = $1
          group by g.game_id
          order by 1`,
        [b.id],
      );
      // SELECTIVITY IS PER NFL WEEK, not per rolling 8 days.
      //
      // "Traded 2 of 16" only means something against a slate, and a rolling
      // window straddles two of them: on a Tuesday it mixed the week that just
      // finished with the one being priced, so the denominator moved for
      // reasons that had nothing to do with the bot being choosier.
      //
      // The current week is the earliest week still holding an unplayed game,
      // which is correct mid-slate too -- on a Friday, Thursday's game is done
      // and Sunday's are not, and both belong to the week being reported.
      // Falling back to the last week played keeps it sane in the offseason.
      const { rows: weekRows } = await pg.query(
        `select season, week from sports.nfl_games
           where kickoff > now() and game_type = 'REG'
           order by kickoff limit 1`,
      );
      const { rows: lastWeekRows } = weekRows.length ? { rows: [] } : await pg.query(
        `select season, week from sports.nfl_games
           where home_score is not null and game_type = 'REG'
           order by kickoff desc limit 1`,
      );
      const cur = weekRows[0] || lastWeekRows[0] || null;

      // A GAME THAT HAS NOT KICKED OFF IS NOT A PASS. Counting the whole slate
      // the moment it is first evaluated reports "1 of 16" on a Thursday night
      // when fifteen of those games can still be traded, which reads as a bot
      // that declined them. The denominator is games whose chance has gone --
      // kicked off -- plus any it has already taken a position in, so a trade
      // placed on Friday for Sunday cannot produce "1 of 0".
      const { rows: selRows } = await pg.query(
        `select count(distinct d.game_id) as looks,
                count(distinct d.game_id) filter (where d.acted) as acted
           from bots.decisions d
           join sports.nfl_games g on g.game_id = d.game_id
          where d.bot_id = $1 and g.season = $2 and g.week = $3
            and (g.kickoff <= now() or d.acted)`,
        [b.id, cur?.season ?? 0, cur?.week ?? 0],
      );

      const startNav = Number(b.starting_nav);

      // NAV IS COMPUTED HERE, NOT READ FROM THE LAST SNAPSHOT.
      //
      // bots.nav_history gains a row when run/daily.js runs, once a day. Reading
      // the latest row meant every settlement, exit and price move after that run
      // was invisible until the next one: Adam-7 showed $10,217.47 and +2.17% ROI
      // while its own realised P&L said +$303.53 and it held nothing -- the two
      // figures on the same card disagreed by $86, because one was live and the
      // other was eighteen hours old.
      //
      // Same arithmetic as accounting/nav.js snapshotNav, evaluated now: starting
      // capital, minus what open positions cost, plus realised P&L, plus open
      // positions marked to the last recorded book.
      const { rows: liveRows } = await pg.query(
        `select
           coalesce((select sum(p.cost_usd) from bots.positions p
                      where p.bot_id = $1 and p.shares > 0), 0) AS open_cost,
           coalesce((select sum(t.pnl_usd) from bots.trades t
                      where t.bot_id = $1 and t.settled), 0) AS realized,
           coalesce((select sum(p.shares * coalesce(
                        (select o.mid from sports.odds_history o
                          where o.token_id = p.token_id and o.mid is not null
                          order by o.ts desc limit 1),
                        p.avg_price))
                       from bots.positions p
                      where p.bot_id = $1 and p.shares > 0), 0) AS open_value`,
        [b.id],
      );
      const live = liveRows[0];
      const nav =
        startNav - Number(live.open_cost) + Number(live.realized) + Number(live.open_value);

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
          /** The slate those counts describe, so the UI can name it. */
          season: cur ? Number(cur.season) : null,
          week: cur ? Number(cur.week) : null,
        },
        // Today's point is replaced with the live NAV below, so the curve does
        // not flatten between daily runs.
        navSeries: buildNavSeries(navRows, intraRows, nav, windowRows).map((r) => ({
          d: r.d,
          nav: r.nav_usd,
          idle: r.idle === true,
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
 * Trade log -- ONE ROW PER POSITION, not per event.
 *
 * An earlier version fanned each position into a BUY row and a SELL row. That
 * gave one game two lines and split numbers that only mean something together:
 * an entry price is not interesting on its own, it is interesting next to what
 * the position closed at.
 *
 * A position closes one of two ways and both collapse into exitPrice:
 *   - exit_price set -> the resting sell filled at fair value before kickoff
 *   - settled        -> the game decided it; the share paid 1.00 or 0.00
 *
 * Still open -> exitPrice, pnlUsd and returnPct are null rather than 0, so the
 * UI shows a dash instead of implying a flat trade.
 */
/**
 * One series for the chart: daily points for the long run, intraday for the
 * recent window, and the live NAV as the final point.
 *
 * WHY BOTH. bots.nav_history is keyed by day and is what the track record and
 * "best day" read -- one authoritative close per day is right for that. It is
 * wrong for a chart someone is watching, where a position moving all afternoon
 * would appear as a single step at midnight. So the daily series is truncated
 * where the intraday one begins and they are concatenated.
 *
 * The live point matters independently: without it the curve flatlines from the
 * last recorded point to the right edge while the ROI beside it reads something
 * else, which is what made this page look frozen in the first place.
 */
type NavPoint = {
  d: string;
  nav_usd: number;
  open_positions: number;
  /** A bridging point standing in for a stretch when the bot held nothing. */
  idle?: boolean;
  /** A daily close rather than an intraday sample; judged by day, not instant. */
  daily?: boolean;
};

/** Merge overlapping windows so a Sunday slate reads as one session, not twelve. */
function mergeWindows(rows: any[]): { start: number; end: number }[] {
  const ws = rows
    .map((r) => ({
      start: new Date(r.w_start).getTime(),
      end: new Date(r.w_end).getTime(),
    }))
    .filter((w) => Number.isFinite(w.start) && Number.isFinite(w.end))
    .sort((x, y) => x.start - y.start);

  const out: { start: number; end: number }[] = [];
  for (const w of ws) {
    const last = out[out.length - 1];
    if (last && w.start <= last.end) last.end = Math.max(last.end, w.end);
    else out.push({ ...w });
  }
  return out;
}

/**
 * Collapse the dead time between games.
 *
 * Both charts place points by INDEX -- x = i / (n - 1) -- so every sample costs
 * the same horizontal space whether anything happened or not. This bot is exposed
 * for a few hours on a Sunday and flat for the rest of the week, so a straight
 * time series spends most of its width drawing a ruler and compresses the part
 * worth looking at into a sliver.
 *
 * So: keep every point inside an active window, and collapse each idle stretch to
 * a SINGLE bridging point carrying the level it ended at. The line stays continuous
 * and honest about the value -- NAV never jumps -- but a four-day gap costs one
 * index step instead of four hundred. Same idea as an equity chart skipping
 * overnights and weekends.
 *
 * Bridging points are flagged `idle` so the frontend can draw them differently
 * later; nothing reads it yet, and the compression alone fixes the shape.
 *
 * With no windows at all -- a bot that has never traded -- this returns the series
 * untouched rather than emptying the chart.
 */
function compressIdle(points: NavPoint[], windows: { start: number; end: number }[]): NavPoint[] {
  if (!windows.length || points.length < 3) return points;

  const DAY_MS = 86_400_000;
  // A daily close is stamped at midnight, and midnight is never inside a game --
  // so judging it by instant would mark every pre-intraday point idle and collapse
  // a bot's whole history to a couple of points. Judge those by whether their DAY
  // overlaps a window; judge intraday samples by the instant, which is the whole
  // point of having them.
  const active = (p: NavPoint) => {
    const ms = new Date(p.d).getTime();
    if (!p.daily) return windows.some((w) => ms >= w.start && ms <= w.end);
    return windows.some((w) => ms < w.end && ms + DAY_MS > w.start);
  };

  const out: NavPoint[] = [];
  let pendingIdle: NavPoint | null = null;

  for (const p of points) {
    if (active(p)) {
      // Flush the idle stretch that led into this window, so the line enters it
      // from the level it actually sat at rather than jumping.
      if (pendingIdle) { out.push({ ...pendingIdle, idle: true }); pendingIdle = null; }
      out.push(p);
    } else {
      pendingIdle = p;   // keep only the most recent; earlier ones are the same line
    }
  }
  // A trailing idle stretch is the present -- always worth showing.
  if (pendingIdle) out.push({ ...pendingIdle, idle: true });

  // Guarded on the RESULT, not on how much was active. A young bot legitimately
  // compresses to very few points -- Adam-7's whole history is a launch level and
  // two step-ups -- and that is a better chart than the same information stretched
  // over a 26-sample flat line. Only bail out if there would be too little left to
  // draw a curve at all.
  return out.length >= 3 ? out : points;
}
function buildNavSeries(
  navRows: any[],
  intraRows: any[],
  liveNav: number,
  windowRows: any[] = [],
): NavPoint[] {
  const iso = (v: any) =>
    v instanceof Date ? v.toISOString() : new Date(v).toISOString();
  const day = (v: any) =>
    v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10);

  const intra = intraRows.map((r) => ({
    d: iso(r.ts),
    nav_usd: Number(r.nav_usd),
    open_positions: r.open_positions,
  }));

  // Daily points only up to where the intraday window starts, so the two do not
  // both describe the same afternoon.
  const cutoff = intra.length ? day(intraRows[0].ts) : null;
  const daily = navRows
    .filter((r) => !cutoff || day(r.d) < cutoff)
    .map((r) => ({
      d: `${day(r.d)}T00:00:00.000Z`,
      nav_usd: Number(r.nav_usd),
      open_positions: r.open_positions,
      daily: true,
    }));

  const out = [...daily, ...intra];

  // The live figure, as the last point. Replaces the newest point when that is
  // already within a tick, rather than stacking a near-duplicate on top of it.
  const nowIso = new Date().toISOString();
  const last = out[out.length - 1];
  const lastAgeMs = last ? Date.now() - new Date(last.d).getTime() : Infinity;
  if (last && lastAgeMs < 60_000) last.nav_usd = liveNav;
  else out.push({ d: nowIso, nav_usd: liveNav, open_positions: 0 });

  return compressIdle(out, mergeWindows(windowRows));
}

botsRouter.get("/:botId/trades", async (req, res) => {
  try {
    const limit = Math.min(200, Number(req.query.limit) || 50);
    const { rows } = await pg.query(
      `select t.id, t.game_id, t.side, t.settled, t.won, t.exited,
              t.market_type, t.line,
              t.p_fair, t.fill_price, t.shares, t.notional_usd,
              t.exit_price, t.exit_at, t.exit_reason,
              t.pnl_usd, t.clv_bps, t.opened_at, t.closed_at,
              g.away_team, g.home_team, g.kickoff, g.week, g.season,
              -- Live mark for an OPEN position: the last price the depth
              -- recorder saw for this exact token. Lateral rather than a join on
              -- max(ts), which would fan out across every snapshot ever taken of
              -- the token -- one row per minute, per market, forever.
              mk.mid AS mark_price, mk.ts AS mark_ts
         from bots.trades t
         join sports.nfl_games g on g.game_id = t.game_id
         left join lateral (
           select o.mid, o.ts from sports.odds_history o
            where o.token_id = t.token_id and o.mid is not null
            order by o.ts desc limit 1
         ) mk on t.settled = false
        where t.bot_id = $1
        order by t.opened_at desc
        limit $2`,
      [req.params.botId, limit],
    );

    const trades = rows.map((t: any) => {
      const entry = Number(t.fill_price);
      const shares = Number(t.shares);
      const cost = Number(t.notional_usd);

      let exit: number | null = null;
      let status = "open";
      if (t.exit_price !== null && t.exit_price !== undefined) {
        exit = Number(t.exit_price);
        status = "sold";
      } else if (t.settled) {
        exit = t.won ? 1 : 0;
        status = t.won ? "won" : "lost";
      }

      const pnl = exit === null ? null : Number(t.pnl_usd ?? (exit - entry) * shares);
      const returnPct = exit === null || !entry ? null : ((exit - entry) / entry) * 100;

      // UNREALISED, for positions still open. Marked to the last recorded book,
      // the same source bots.nav_history marks NAV to, so the trade log and the
      // portfolio figure cannot tell different stories about the same position.
      // Null once a position is closed -- it has a realised number by then, and
      // showing both invites reading the stale one.
      const mark = t.mark_price === null || t.mark_price === undefined
        ? null : Number(t.mark_price);
      const isOpen = status === "open";
      // Resting SELL the bot holds on an open position: fair value of the TOKEN
      // it owns. p_fair is stored for the home/over side, so flip it for
      // away/under to get the held token's fair.
      const pFair = t.p_fair === null || t.p_fair === undefined ? null : Number(t.p_fair);
      const heldFair = pFair === null
        ? null
        : (t.side === "home" || t.side === "over" ? pFair : 1 - pFair);
      const markValueUsd = isOpen && mark !== null ? mark * shares : null;
      const unrealizedPnlUsd = isOpen && mark !== null ? (mark - entry) * shares : null;
      const unrealizedPct = isOpen && mark !== null && entry
        ? ((mark - entry) / entry) * 100 : null;

      // WHAT THE POSITION ACTUALLY IS, per market type.
      //
      // `team` used to be the only description, computed as
      // `side === "home" ? home_team : away_team`. For a totals trade side is
      // "over" or "under", so it fell through to the AWAY team -- an Argo-7 bet
      // on Over 46.5 in NE @ JAX rendered as "NE", which does not merely read as
      // unclear, it reads as a moneyline bet on New England.
      const isTotals = t.market_type === "totals";
      const line = t.line === null || t.line === undefined ? null : Number(t.line);
      const position = isTotals
        ? `${t.side === "over" ? "Over" : "Under"}${line === null ? "" : " " + line}`
        : (t.side === "home" ? t.home_team : t.away_team);

      return {
        tradeId: t.id,
        gameId: t.game_id,
        matchup: `${t.away_team} @ ${t.home_team}`,
        marketType: t.market_type || "moneyline",
        line,
        // The label to show. Never a team code for a totals position.
        position,
        // Kept for the moneyline bots, and deliberately NULL for totals so that
        // nothing downstream can render a team code for an over/under position.
        team: isTotals ? null : (t.side === "home" ? t.home_team : t.away_team),
        side: t.side,
        week: t.week,
        season: t.season,
        kickoff: t.kickoff,
        openedAt: t.opened_at,
        closedAt: t.closed_at,
        entryPrice: entry,
        exitPrice: exit,
        shares,
        costUsd: cost,
        pnlUsd: pnl,
        returnPct,
        /** Last recorded book mid. Null unless the position is still open. */
        markPrice: mark !== null && isOpen ? mark : null,
        markAt: isOpen && t.mark_ts ? t.mark_ts : null,
        /** What the open position is worth right now. */
        markValueUsd,
        unrealizedPnlUsd,
        unrealizedPct,
        /** Resting sell (limit) price the model holds on an OPEN position =
         *  fair value of the owned token. Null once the position closes. */
        limitSellPrice: isOpen && heldFair !== null ? heldFair : null,
        status,
        exitReason: t.exit_reason || null,
        clvBps: t.clv_bps === null ? null : Number(t.clv_bps),
      };
    });

    res.json({ trades });
  } catch (e: any) {
    console.error("[bots] trades failed:", e?.message || e);
    res.status(500).json({ error: "bots_trades_unavailable" });
  }
});


export default botsRouter;
