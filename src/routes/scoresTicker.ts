// src/routes/scoresTicker.ts
//
// Slim feed for the home-page score ticker.
//
// WHY THIS EXISTS. The ticker used to read game_score_cache straight from
// Supabase. Two problems with that:
//
//   1. It shipped the raw Goalserve envelopes -- ~44 kB of JSON per game,
//      40 games a poll, every 60s, to render fifteen lines of "TEAM 4 - 2
//      TEAM". Around 1.7 MB per visitor per minute.
//   2. It never triggered a refresh. game_score_cache is revalidated lazily
//      by /api/scores/live when a market page is viewed, so games nobody
//      opened stayed frozen at "Top 1st" forever and sorted to the FRONT of
//      the ticker, because non-final rows sort first.
//
// This endpoint projects the handful of fields the ticker draws (a few hundred
// bytes a game) and nudges the stalest rows through the SAME revalidation path
// the market pages use, so the cache self-heals from ticker traffic too.
//
// It is deliberately small. The ticker is decorative -- every failure path
// here degrades to "return what we have" rather than erroring.

import { Router } from "express";
import { pg } from "../db/pg";
import { revalidateScoreCache } from "./scores";

const scoresTickerRouter = Router();

/** A non-final row this long after its game is a zombie, not a live game. */
const ZOMBIE_AFTER_MS = 12 * 60 * 60 * 1000;
/** Serve the same payload to everyone for this long. */
const CACHE_MS = 30_000;
/** Hard ceiling on background refreshes per request -- Goalserve is rate-limited
 *  per key, and a busy page must not turn into a fan-out of API calls. */
const MAX_REVALIDATE = 3;
/** Only bother refreshing a non-final row once it is this stale. */
const REVALIDATE_AFTER_MS = 90_000;

let cache: { at: number; body: any } | null = null;

/**
 * Goalserve returns "26.09.2026 02:10" -- DD.MM.YYYY, in UTC. Date.parse reads
 * that as either MM.DD or NaN depending on the runtime, so it has to be parsed
 * explicitly. Getting this wrong is not cosmetic: gameTs drives both the
 * live-first ordering and the zombie cutoff, and a silent fallback to
 * fetched_at would make a stale row look freshly played.
 */
function parseGoalserveUtc(v: string | null): number {
  if (!v) return 0;
  const m = /^(d{2}).(d{2}).(d{4})(?:s+(d{2}):(d{2}))?$/.exec(v.trim());
  if (m) {
    const [, dd, mm, yyyy, hh = "00", mi = "00"] = m;
    return Date.UTC(+yyyy, +mm - 1, +dd, +hh, +mi);
  }
  // ISO or anything else the runtime understands ("Sep 25, 2026").
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : 0;
}

scoresTickerRouter.get("/", async (req, res) => {
  try {
    const limit = Math.min(30, Number(req.query.limit) || 15);
    if (cache && Date.now() - cache.at < CACHE_MS) return res.json(cache.body);

    // Project in SQL so the 44 kB envelopes never enter this process.
    // totalscore / name are common to MLB, NBA, NFL and NHL; the coalesce
    // covers soccer shapes that use score/goals instead.
    const { rows } = await pg.query(`
      with recent as (
        select contract_address, league, is_final, fetched_at,
               score_data->'scores'->'category'->'match' as m
          from public.game_score_cache
         order by fetched_at desc
         limit 60
      )
      select contract_address, league, is_final, fetched_at,
             coalesce(m->>'status', '')                as status,
             coalesce(m->>'datetime_utc', m->>'date')  as game_at,
             m->'hometeam'->>'name'                    as home_label,
             m->'awayteam'->>'name'                    as away_label,
             coalesce(m->'hometeam'->>'totalscore',
                      m->'hometeam'->>'score',
                      m->'hometeam'->>'goals', '')     as home_score,
             coalesce(m->'awayteam'->>'totalscore',
                      m->'awayteam'->>'score',
                      m->'awayteam'->>'goals', '')     as away_score
        from recent
       where m is not null
    `);

    const now = Date.now();
    const parsed = rows
      .map((r: any) => {
        const ts = parseGoalserveUtc(r.game_at) || Date.parse(r.fetched_at) || 0;
        return {
          contractAddress: r.contract_address,
          league: r.league,
          homeLabel: r.home_label || "",
          awayLabel: r.away_label || "",
          homeScore: r.home_score || "",
          awayScore: r.away_score || "",
          status: r.status || "",
          isFinal: !!r.is_final,
          fetchedAt: r.fetched_at,
          gameTs: ts,
          _ageMs: now - new Date(r.fetched_at).getTime(),
        };
      })
      .filter((x: any) => x.homeLabel && x.awayLabel)
      // Drop zombies: no final score long after the game. We do not know the
      // result, and a wrong score is worse than one fewer game.
      .filter((x: any) => x.isFinal || now - x.gameTs < ZOMBIE_AFTER_MS);

    // Live first, then most recent finals.
    parsed.sort((a: any, b: any) =>
      a.isFinal !== b.isFinal ? (a.isFinal ? 1 : -1) : b.gameTs - a.gameTs);

    const items = parsed.slice(0, limit);

    // Nudge the stalest non-final rows through the market pages' own
    // revalidation path. Fire-and-forget: the response is already built.
    const stale = items
      .filter((x: any) => !x.isFinal && x._ageMs > REVALIDATE_AFTER_MS)
      .sort((a: any, b: any) => b._ageMs - a._ageMs)
      .slice(0, MAX_REVALIDATE);

    for (const s of stale) {
      const lockTime = Math.floor(s.gameTs / 1000);
      void revalidateScoreCache(
        s.league, s.awayLabel, s.homeLabel, lockTime, s.contractAddress,
      ).catch(() => { /* decorative feed; a failed refresh just leaves the row */ });
    }

    const body = { items: items.map(({ _ageMs, ...rest }: any) => rest) };
    cache = { at: Date.now(), body };
    res.json(body);
  } catch (e: any) {
    console.error("[scores/ticker] failed:", e?.message || e);
    // Never 500 the ticker -- an empty list just hides it.
    res.json({ items: [] });
  }
});

export default scoresTickerRouter;
