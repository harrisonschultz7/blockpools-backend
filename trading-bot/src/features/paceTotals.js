// trading-bot/src/features/paceTotals.js
//
// PACE (weight 0.10) and REST (0.15) and SITUATIONAL (0.08) for a game total.
// All three read sports.nfl_team_game_stats and sports.nfl_games, and all three
// return POINTS, positive meaning more points.
//
// Grouped in one file because they are small and share their data sources, the
// same way features/situational.js bundles rest, travel and weather for Adam-7.
//
// PACE IS LOW-WEIGHTED ON PURPOSE, and the reason is double-counting rather than
// weak signal. Pace is the most mechanically direct driver of a total -- more
// snaps means more scoring chances -- which is exactly why the market prices it
// well. It keeps a small weight of its own because the user wants the
// pace-specific part represented above whatever the anchor already carries, but
// a large weight here would mostly re-bet the market's own view back at it.
//
// SUMS, NOT DIFFERENTIALS. This is the structural difference from Adam-7
// throughout. A moneyline model wants home minus away; a total wants home plus
// away. Two fast offences produce more plays than one fast and one slow, and a
// differential model is blind to exactly that game.

const { cfg } = require("../config");
const { q } = require("../db");

/**
 * Recency-weighted seconds-per-play for both teams, from games before asOf.
 *
 * Shrunk toward the league average by shrinkGames, so a team with two games on
 * record is not read as having a settled tempo.
 */
async function loadPace(asOf, startSeason) {
  const { rows } = await q(
    `select s.team, s.sec_per_play, s.off_plays, g.kickoff
       from sports.nfl_team_game_stats s
       join sports.nfl_games g on g.game_id = s.game_id
      where s.season >= $1 and g.kickoff < $2 and s.sec_per_play is not null
      order by g.kickoff`,
    [startSeason, asOf],
  );
  const byTeam = new Map();
  for (const r of rows) {
    if (!byTeam.has(r.team)) byTeam.set(r.team, []);
    byTeam.get(r.team).push(Number(r.sec_per_play));
  }
  const league = rows.length
    ? rows.reduce((s, r) => s + Number(r.sec_per_play), 0) / rows.length
    : null;
  return { byTeam, league, n: rows.length };
}

function teamPace(ctx, team, c) {
  const xs = ctx.byTeam.get(team) || [];
  const league = ctx.league ?? c.leagueAvgSecPerPlay;
  if (!xs.length) return { secPerPlay: league, games: 0 };
  const mean = xs.reduce((s, x) => s + x, 0) / xs.length;
  // Shrink toward league on game count.
  const k = c.secPerPlayShrinkGames;
  return {
    secPerPlay: (mean * xs.length + league * k) / (xs.length + k),
    games: xs.length,
  };
}

/**
 * Pace signal in points.
 *
 * Both teams' seconds-per-play are converted into an expected play count for the
 * game, compared with the league-average play count, and the surplus plays are
 * priced at pointsPerExtraPlay. Working in seconds-per-play rather than in plays
 * directly matters because the clock is the binding constraint -- a game has a
 * fixed number of seconds, and tempo decides how many snaps fit in them.
 */
function paceSignal(game, ctx) {
  const c = cfg().model.pace;
  const league = ctx.league ?? c.leagueAvgSecPerPlay;

  const h = teamPace(ctx, game.home_team, c);
  const a = teamPace(ctx, game.away_team, c);

  // Average tempo of the two offences, versus league. Faster (fewer seconds per
  // play) means more snaps in the same clock.
  const gameSecPerPlay = (h.secPerPlay + a.secPerPlay) / 2;
  const leaguePlays = cfg().model.expectedPlaysPerTeam * 2;
  // Plays scale inversely with seconds per play at fixed clock.
  const expectedPlays = leaguePlays * (league / gameSecPerPlay);
  const extraPlays = expectedPlays - leaguePlays;

  const raw = extraPlays * c.pointsPerExtraPlay;
  const points = Math.max(-c.maxPoints, Math.min(c.maxPoints, raw));

  const gamesSeen = Math.min(h.games, a.games);
  const confidence = Math.max(0, Math.min(1, gamesSeen / c.secPerPlayShrinkGames));

  return {
    points,
    confidence,
    detail: {
      homeSecPerPlay: +h.secPerPlay.toFixed(2),
      awaySecPerPlay: +a.secPerPlay.toFixed(2),
      leagueSecPerPlay: +league.toFixed(2),
      expectedPlays: +expectedPlays.toFixed(1),
      extraPlays: +extraPlays.toFixed(1),
      uncapped: +raw.toFixed(3),
      homeGames: h.games, awayGames: a.games,
    },
  };
}

/**
 * REST signal in points (weight 0.15, the user's third-ranked factor).
 *
 * Again a SUM: total rest across both teams relative to a normal week, because
 * two rested offences score more than one rested and one on a short week.
 *
 * Honest caveat, recorded here rather than discovered later: rest is much better
 * established as a predictor of MARGIN than of TOTAL. Two tired teams play a
 * sloppier game, which cuts both ways -- more turnovers and worse execution, but
 * also worse defence. So the coefficient is deliberately small and the cap tight,
 * and if this factor ends up carrying real P&L that is a reason to look for a
 * bug before congratulating it.
 */
/**
 * Number(), but null and empty string are MISSING rather than zero.
 *
 * Number(null) is 0, which is finite, so a null rest day sailed past a
 * Number.isFinite guard and was read as ZERO DAYS OF REST -- the most extreme
 * short week possible -- quietly pushing the total down by 1.3 points on any game
 * with an incomplete row. Caught by test/factors.test.js, not by inspection.
 */
function numOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function restSignal(game) {
  const c = cfg().model.rest;
  const homeRest = numOrNull(game.home_rest);
  const awayRest = numOrNull(game.away_rest);
  if (homeRest === null || awayRest === null) {
    return { points: 0, confidence: 0.5, detail: { reason: "rest days missing" } };
  }

  const parts = {};
  // A normal NFL week is 7 days; the sum is measured against 14.
  const netSum = homeRest + awayRest - 14;
  parts.restSum = netSum * c.pointsPerNetRestDaySum;

  // A short week is worse than the day count implies: less practice, less
  // recovery, a thinner playbook. Applied per team, so a Thursday game between
  // two short-week teams takes it twice.
  parts.shortWeek = 0;
  for (const r of [homeRest, awayRest]) {
    if (r <= c.shortWeekThresholdDays) parts.shortWeek += c.shortWeekPoints;
    else if (r >= c.byeWeekThresholdDays) parts.shortWeek += c.byeWeekPoints;
  }

  const raw = parts.restSum + parts.shortWeek;
  const points = Math.max(-c.maxPoints, Math.min(c.maxPoints, raw));
  return {
    points,
    confidence: 1,
    // Two sub-terms, calibrated separately. They can genuinely pull against each
    // other -- a bye-week bonus and a short-week penalty are different claims --
    // and when they were lumped into one number the combined factor fitted with
    // the WRONG SIGN. Splitting them says which half, if either, is real.
    parts: { restSum: parts.restSum, shortWeek: parts.shortWeek },
    detail: { homeRest, awayRest, netSum,
              restSumPoints: +parts.restSum.toFixed(3),
              shortWeekPoints: +parts.shortWeek.toFixed(3),
              uncapped: +raw.toFixed(3) },
  };
}

/**
 * SITUATIONAL signal in points (weight 0.08 -- "situational doesn't matter in my
 * opinion, very low weight").
 *
 * Two terms only. Division familiarity compresses scoring slightly, and
 * primetime games skew a little lower. Roof is deliberately NOT here even though
 * indoor games score more: everyone can see the roof, so it lives in the anchor,
 * and pricing it again would be the easiest available way to lose money.
 */
function situationalTotalsSignal(game) {
  const c = cfg().model.situational;
  const parts = {};
  let raw = 0;

  if (game.div_game) { parts.division = c.divisionGamePoints; raw += c.divisionGamePoints; }

  const isPrimetime = /^(19|20|21|22):/.test(String(game.gametime || ""));
  if (isPrimetime) { parts.primetime = c.primetimePoints; raw += c.primetimePoints; }

  const points = Math.max(-c.maxPoints, Math.min(c.maxPoints, raw));
  return {
    points,
    confidence: 1,
    parts: { division: parts.division || 0, primetime: parts.primetime || 0 },
    detail: { ...parts, divGame: !!game.div_game, isPrimetime, uncapped: +raw.toFixed(3) },
  };
}

module.exports = { loadPace, paceSignal, restSignal, situationalTotalsSignal, teamPace, numOrNull };
