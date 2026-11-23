// trading-bot/src/features/schemeMatchup.js
//
// SCHEME MATCHUP for a game total (Argo-7), in points against the market line.
//
// The stance (user, 2026-11-23): the DEFENCE wins two style matchups and the
// closing line does not fully price it.
//
//   1. Play-action offence  x  defence that loads the box (heavy-box rate).
//      Play-action sells the run; a loaded box stops the run anyway.
//   2. Pass-heavy offence   x  defence that rushes few and drops into coverage
//      (low pass rushers per dropback).
//
// Each is measured for both offences against the opposing defence and summed,
// in z-scores, so a strong defensive matchup is a POSITIVE signal and pushes
// the total DOWN; the reverse (play-action into a light box, pass-heavy into a
// blitzing defence) pushes it UP.
//
// Evidence (scratchpad scheme_totals.js / scheme_combo.js, 1,238 games 2022-26,
// pre-game traits only): -0.92 points vs the closing total per unit of signal,
// t -3.6; same sign in 4 of 5 seasons (2024 flat); 2025-26 alone -1.37, t -3.1;
// top 20% of games went over 44%, bottom 20% 52%. Nine ideas were tried and only
// these two held their sign in both 2022-24 and 2025-26, so the live coefficient
// is shrunk to -0.6 (config model.schemeMatchup.pointsPerUnit). Replay of 2026
// at a 2.5c bar: 22 trades, 13-9, +15.9% held to settlement (in-sample).
//
// Traits are each team's own average over its earlier games THIS season,
// blended with last season until it has 8 games (no look-ahead: only games
// that kicked off before this one count).

const { cfg } = require("../config");
const { cachedRows } = require("../cache");

/** Every team-game row from last season on, for the traits. Loaded once per tick. */
async function loadSchemeMatchupContext(season) {
  // Through the disk cache (src/cache.js): the charting only changes when the
  // daily FTN/nflverse ingest runs.
  const rows = await cachedRows("scheme-matchup",
    `select s.team, s.season, extract(epoch from g.kickoff) * 1000 as k,
            s.off_pa_rate::float8 off_pa_rate, s.def_heavy_box_rate::float8 def_heavy_box_rate,
            s.def_rushers_avg::float8 def_rushers_avg, t.off_pass_rate::float8 off_pass_rate
       from sports.nfl_team_scheme_game s
       join sports.nfl_games g on g.game_id = s.game_id
       left join sports.nfl_team_game_stats t on t.game_id = s.game_id and t.team = s.team
      where s.season >= $1
      order by g.kickoff`,
    [season - 1],
    { keyParams: [season - 1] },
  );
  const byTeam = new Map();
  for (const r of rows) {
    const arr = byTeam.get(r.team) || [];
    arr.push({ ...r, k: Number(r.k) });
    byTeam.set(r.team, arr);
  }
  return { byTeam, season };
}

const TRAITS = ["off_pa_rate", "def_heavy_box_rate", "off_pass_rate", "def_rushers_avg"];

function traitsBefore(ctx, team, season, k) {
  const h = (ctx.byTeam.get(team) || []).filter((x) => x.k < k);
  const cur = h.filter((x) => x.season === season);
  const prev = h.filter((x) => x.season === season - 1);
  if (cur.length < 3 && prev.length < 6) return null;
  const avg = (arr, c) => {
    const v = arr.map((x) => x[c]).filter((x) => x != null && Number.isFinite(x));
    return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
  };
  const w = Math.min(1, cur.length / 8);
  const out = { games: cur.length, priorGames: prev.length };
  for (const c of TRAITS) {
    const a = avg(cur, c), b = avg(prev, c);
    out[c] = a == null ? b : b == null ? a : w * a + (1 - w) * b;
  }
  return out;
}

function schemeMatchupSignal(game, ctx) {
  const sm = cfg().model.schemeMatchup;
  const k = new Date(game.kickoff).getTime();
  const season = Number(game.season) || ctx.season;
  const H = traitsBefore(ctx, game.home_team, season, k);
  const A = traitsBefore(ctx, game.away_team, season, k);
  if (!H || !A) {
    return { points: 0, confidence: 0, detail: { reason: "not enough charted games", home: !!H, away: !!A } };
  }
  const z = (c, v) => (v == null ? 0 : (v - sm.traits[c].mean) / sm.traits[c].sd);
  // 1. play-action into a loaded box, both ways.
  const paBox = z("off_pa_rate", H.off_pa_rate) * z("def_heavy_box_rate", A.def_heavy_box_rate)
              + z("off_pa_rate", A.off_pa_rate) * z("def_heavy_box_rate", H.def_heavy_box_rate);
  // 2. pass-heavy into a coverage (few-rushers) defence, both ways.
  const passCoverage = -z("off_pass_rate", H.off_pass_rate) * z("def_rushers_avg", A.def_rushers_avg)
                       - z("off_pass_rate", A.off_pass_rate) * z("def_rushers_avg", H.def_rushers_avg);
  const signal = paBox / sm.paBoxSd + passCoverage / sm.passCoverageSd;
  const raw = sm.pointsPerUnit * signal;
  const points = Math.max(-sm.maxPoints, Math.min(sm.maxPoints, raw));
  return {
    points,
    confidence: 1,
    detail: {
      signal: +signal.toFixed(3),
      playActionVsBox: +(paBox / sm.paBoxSd).toFixed(3),
      passVsCoverage: +(passCoverage / sm.passCoverageSd).toFixed(3),
      uncapped: +raw.toFixed(3),
      home: Object.fromEntries(Object.entries(H).map(([kk, v]) => [kk, typeof v === "number" ? +v.toFixed(4) : v])),
      away: Object.fromEntries(Object.entries(A).map(([kk, v]) => [kk, typeof v === "number" ? +v.toFixed(4) : v])),
    },
  };
}

module.exports = { loadSchemeMatchupContext, schemeMatchupSignal, traitsBefore };
