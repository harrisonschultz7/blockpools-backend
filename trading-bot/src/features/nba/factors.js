// trading-bot/src/features/nba/factors.js
//
// Axis-3's five factors, in the user's order of importance. Every one returns
// POINTS OF HOME MARGIN (positive = good for the home side), its own confidence,
// and a detail object that is stored with the forecast so any trade can be
// audited back to the players and games behind it.
//
//   injury     players in / out RELATIVE TO WHAT THE MARKET HAS SEEN
//   momentum   win streaks + recent margin against season margin
//   rest       back-to-backs, 3-in-4, rest advantage, travel and time zones
//   primetime  national TV (ESPN / ABC / NBC / Prime / TNT) -- effort + crowd
//   style      pace and 3-point volume -> how much variance the game carries
//
// THE INJURY FACTOR IS NOT "TEAM MINUS STAR". The price is the anchor and the
// market has read the same injury report: a star who has been out for a week is
// in the line already, and so is one ruled out this morning. Counting either
// again would double-count it -- the trap the NFL README documents. What can be
// edge is the CHANGE the market may not have absorbed yet:
//
//   change   = availability now  -  availability over the last few games
//   fresh    = 0.5 ^ (hours since the status changed / halfLifeHours)
//   points   = impact x change x fresh
//
// so a late scratch counts nearly in full, this morning's report barely at all,
// and a player out for a month counts nothing (he was out in the recent games
// too, so his change is zero).

const { cfg } = require("../../config");
const { milesBetween, hoursEast } = require("./arenas");
const { isPrimetime } = require("../../ingest/espnNba");

const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const num = (v) => (v === null || v === undefined ? null : Number(v));

// ── Injury ──────────────────────────────────────────────────────────────────

/**
 * A player's worth in points of margin per game, BASELINED like Adam-7's:
 *
 *   prior    = (mpg / 48) x (per-48 value from minutes + shrunk on-court +/-)
 *   measured = team margin with him - team margin without him
 *   impact   = measured, shrunk toward prior by n_without / (n_without + k)
 *
 * Minutes are the coach's revealed view of a player, so they carry the prior;
 * +/- is noisy, so it is shrunk hard toward zero by minutes played. The
 * with/without gap is the honest test -- how the team actually did without him
 * -- but on two or three games it is noise, hence the shrink.
 */
function playerImpact(p, teamMargin, ic) {
  const gp = Number(p.gp);
  if (!(gp > 0)) return 0;
  const minSum = Number(p.min_sum);
  const mpg = minSum / gp;
  const onPer48 = minSum > 0 ? (Number(p.pm_sum) / minSum) * 48 : 0;
  const rel = onPer48 - teamMargin;
  const relShrunk = rel * (minSum / (minSum + ic.pmShrinkMinutes));
  const per48 = ic.per48PerMpg * (mpg - ic.replacementMpg) + relShrunk;
  const prior = clamp((mpg / 48) * per48, 0, ic.maxPlayerPts);

  // The with/without gap only means something for a rotation player: a
  // 10-minute man's "team without him" is mostly blowouts and rest nights.
  const nWithout = Number(p.n_without);
  const withM = num(p.margin_with), withoutM = num(p.margin_without);
  if (mpg >= ic.measuredMinMpg && nWithout > 0 && withM !== null && withoutM !== null) {
    const measured = clamp(withM - withoutM, -ic.maxPlayerPts, ic.maxPlayerPts);
    const w = nWithout / (nWithout + ic.shrinkGamesWithout);
    return clamp(w * measured + (1 - w) * prior, 0, ic.maxPlayerPts);
  }
  return prior;
}

function teamInjuryPts(team, ctx, now, ic) {
  const players = ctx.players[team] || [];
  const games = ctx.games[team] || [];
  const teamMargin = games.length
    ? games.slice(0, ic.lookbackGames).reduce((s, g) => s + Number(g.margin), 0) / Math.min(games.length, ic.lookbackGames)
    : 0;
  const report = new Map((ctx.injuries[team] || []).map((i) => [String(i.player_id), i]));

  let pts = 0;
  const moves = [];
  for (const p of players) {
    const impact = playerImpact(p, teamMargin, ic);
    if (impact < ic.minImpactPts) continue;
    // Share of his recent games he was AVAILABLE for (injury/illness absences
    // only -- see context.js `unavailable`).
    const rowsRecent = Number(p.rows_recent);
    const availRecent = rowsRecent > 0 ? 1 - Number(p.out_recent) / rowsRecent : 1;
    const inj = report.get(String(p.player_id));
    const mult = inj ? (ic.statusMultiplier[String(inj.status).toUpperCase()] ?? ic.statusMultiplier.default) : 0;
    const availNow = 1 - mult;
    const change = availNow - availRecent;
    if (Math.abs(change) < 0.05) continue;

    // How new is this news? A status on the report: hours since IT changed. A
    // return (off the report, but missing lately): unknown, so a fixed share.
    let fresh;
    let ageH = null;
    if (inj) {
      ageH = (now.getTime() - new Date(inj.status_changed_at).getTime()) / 3_600_000;
      fresh = Math.pow(0.5, Math.max(0, ageH) / ic.freshnessHalfLifeHours);
    } else {
      fresh = ic.returnFreshness;
    }
    const d = impact * change * fresh;
    pts += d;
    moves.push({
      player: p.player, impact: +impact.toFixed(2), status: inj ? inj.status : "active",
      availRecent: +availRecent.toFixed(2), availNow: +availNow.toFixed(2),
      ageHours: ageH === null ? null : +ageH.toFixed(1), fresh: +fresh.toFixed(2), pts: +d.toFixed(2),
    });
  }
  return { pts, moves: moves.sort((a, b) => Math.abs(b.pts) - Math.abs(a.pts)).slice(0, 6), nPlayers: players.length };
}

function injurySignal(game, ctx, now) {
  const ic = cfg().model.injury;
  const h = teamInjuryPts(game.home_team, ctx, now, ic);
  const a = teamInjuryPts(game.away_team, ctx, now, ic);
  const pts = clamp(h.pts - a.pts, -ic.maxSwingPts, ic.maxSwingPts);
  // Confidence: rosters known from box scores, and a recent injury feed.
  const feedAgeMin = ctx.injuryFeedAt ? (now.getTime() - ctx.injuryFeedAt.getTime()) / 60000 : Infinity;
  const rosterOk = (n) => clamp(n / 8, 0, 1);
  const confidence = Math.min(rosterOk(h.nPlayers), rosterOk(a.nPlayers)) * (feedAgeMin <= ic.maxFeedAgeMinutes ? 1 : 0.4);
  return { pts, confidence, detail: { home: h.moves, away: a.moves, feedAgeMin: Number.isFinite(feedAgeMin) ? Math.round(feedAgeMin) : null } };
}

// ── Momentum ────────────────────────────────────────────────────────────────

function teamMomentum(team, ctx, mc) {
  const g = (ctx.games[team] || []).filter((x) => x.season === ctx.games[team][0]?.season);
  if (!g.length) return { pts: 0, n: 0, streak: 0 };
  // Current streak, signed: +4 = won four straight.
  const first = Number(g[0].margin) > 0;
  let streak = 0;
  for (const x of g) { if ((Number(x.margin) > 0) === first) streak++; else break; }
  streak = first ? streak : -streak;

  // Recent margin (exponentially weighted) against the season's.
  let wSum = 0, mSum = 0;
  g.slice(0, mc.formGames).forEach((x, i) => {
    const w = Math.pow(0.5, i / mc.halfLifeGames);
    wSum += w; mSum += w * Number(x.margin);
  });
  const recent = wSum ? mSum / wSum : 0;
  const season = g.reduce((s, x) => s + Number(x.margin), 0) / g.length;
  // Few games: the season figure IS the recent figure; shrink the difference.
  const shrink = g.length / (g.length + mc.shrinkGames);
  const formPts = mc.formWeight * (recent - season) * shrink;
  const streakPts = mc.streakPtsPerGame * clamp(streak, -mc.maxStreak, mc.maxStreak);
  return { pts: formPts + streakPts, n: g.length, streak, recent: +recent.toFixed(1), season: +season.toFixed(1) };
}

function momentumSignal(game, ctx) {
  const mc = cfg().model.momentum;
  const h = teamMomentum(game.home_team, ctx, mc);
  const a = teamMomentum(game.away_team, ctx, mc);
  const pts = clamp(h.pts - a.pts, -mc.maxSwingPts, mc.maxSwingPts);
  const confidence = clamp(Math.min(h.n, a.n) / mc.fullConfidenceGames, 0, 1);
  return { pts, confidence, detail: { home: h, away: a } };
}

// ── Rest / travel ───────────────────────────────────────────────────────────

/** Calendar days between two ET dates ("2026-11-01"). */
const dayDiff = (a, b) => Math.round((Date.parse(b) - Date.parse(a)) / 86_400_000);
const etYmd = (d) => (d instanceof Date ? d.toISOString().slice(0, 10) : String(d).slice(0, 10));

function teamRest(team, game, ctx, rc) {
  const prev = (ctx.games[team] || [])[0];               // most recent final before tip
  const today = etYmd(game.game_date);
  if (!prev) return { pts: 0, restDays: null };
  const restDays = dayDiff(etYmd(prev.game_date), today) - 1;   // 0 = back-to-back
  if (restDays > 6) return { pts: 0, restDays };                 // opener / long break

  const last3 = (ctx.games[team] || []).filter((x) => dayDiff(etYmd(x.game_date), today) <= 3).length;
  let pts = 0;
  if (restDays === 0) pts += rc.backToBackPts;
  else if (last3 >= 2) pts += rc.threeInFourPts;         // 3rd game in 4 nights

  // Travel from where the last game was played to tonight's arena.
  const from = prev.neutral_site ? null : prev.home_team;
  const to = game.neutral_site ? null : game.home_team;
  let miles = null, east = 0;
  if (from && to && from !== to) {
    miles = milesBetween(from, to);
    east = hoursEast(from, to);
    if (miles) pts += Math.max(rc.maxTravelPts, rc.travelPtsPer1000Miles * (miles / 1000));
    if (restDays === 0 && east > 0) pts += rc.eastwardB2bPtsPerHour * east;
  }
  return { pts, restDays, miles: miles === null ? null : Math.round(miles), east };
}

function restSignal(game, ctx) {
  const rc = cfg().model.rest;
  const h = teamRest(game.home_team, game, ctx, rc);
  const a = teamRest(game.away_team, game, ctx, rc);
  // Rest advantage on top of the penalties: an extra day matters until ~3.
  const adv = h.restDays !== null && a.restDays !== null
    ? rc.restAdvantagePtsPerDay * (Math.min(h.restDays, 3) - Math.min(a.restDays, 3)) : 0;
  const pts = clamp(h.pts - a.pts + adv, -rc.maxSwingPts, rc.maxSwingPts);
  const confidence = h.restDays !== null && a.restDays !== null ? 1 : 0.5;
  return { pts, confidence, detail: { home: h, away: a, restAdvantagePts: +adv.toFixed(2) } };
}

// ── Primetime ───────────────────────────────────────────────────────────────

/**
 * National TV. Two effects, both small: stars play (load management is rare on
 * the big networks), which favours the better team, and the building is louder,
 * which favours the home team. NBA TV is national but not "primetime".
 */
function primetimeSignal(game, ctx) {
  const pc = cfg().model.primetime;
  if (!isPrimetime(game.national_tv)) return { pts: 0, confidence: 1, detail: { national: game.national_tv || null } };
  const season = (team) => {
    const g = ctx.games[team] || [];
    return g.length ? g.reduce((s, x) => s + Number(x.margin), 0) / g.length : 0;
  };
  const better = Math.sign(season(game.home_team) - season(game.away_team));
  const pts = pc.homeCrowdPts + pc.betterTeamPts * better;
  return { pts, confidence: 1, detail: { national: game.national_tv, betterSide: better > 0 ? "home" : better < 0 ? "away" : "even" } };
}

// ── Style ───────────────────────────────────────────────────────────────────

/**
 * Pace and 3-point volume change how much a game's result VARIES, not who is
 * better. The expected margin grows with possessions while its spread grows
 * with their square root, so a slow game is closer to a coin flip than the same
 * matchup played fast; a game of threes swings more than one of layups. The
 * market's implied margin assumes an average game, so:
 *
 *   factor = sqrt(poss / league_poss) / (1 + k3 x (3PA rate - league rate))
 *   points = implied_margin x (factor - 1)
 *
 * which pushes a fast, inside-heavy game toward the favourite and a slow,
 * three-heavy one toward the underdog. Zero when the market is at 50%.
 */
function styleSignal(game, ctx, impliedMargin) {
  const sc = cfg().model.style;
  const h = ctx.style[game.home_team], a = ctx.style[game.away_team], lg = ctx.league;
  if (!h || !a || !(lg.poss > 0) || h.n < sc.minGames || a.n < sc.minGames) {
    return { pts: 0, confidence: 0.5, detail: { reason: "thin_box_scores" } };
  }
  const poss = (h.poss + a.poss) / 2;
  const par3 = (h.par3 + a.par3) / 2;
  const factor = Math.sqrt(poss / lg.poss) / (1 + sc.threePointVarianceK * (par3 - lg.par3));
  const pts = clamp(impliedMargin * (factor - 1), -sc.maxSwingPts, sc.maxSwingPts);
  return {
    pts, confidence: clamp(Math.min(h.n, a.n) / sc.games, 0, 1),
    detail: { poss: +poss.toFixed(1), leaguePoss: +lg.poss.toFixed(1), par3: +par3.toFixed(3),
              leaguePar3: +lg.par3.toFixed(3), factor: +factor.toFixed(3) },
  };
}

module.exports = { injurySignal, momentumSignal, restSignal, primetimeSignal, styleSignal, playerImpact };
