// trading-bot/src/features/nba/ratings.js
//
// BAIN-3's own view of a game: opponent-adjusted team ratings from box scores,
// with the market nowhere in the calculation.
//
//   offense / defense  points per 100 possessions, adjusted for who was played
//   pace               possessions per game
//   style              3PA rate, free-throw rate, offensive rebounds and
//                      turnovers per 100, both for and ALLOWED
//
// Current-season games dominate, decaying by age. Last season counts only
// lightly (priorSeasonWeight), because rosters move: Giannis is in Miami now.
// Every rating is pulled toward the league by shrinkGames, so a team eight games
// in is not read as a settled number.
//
// EGRESS: the box-score aggregate is ~3,000 rows, so it is read once per
// cacheHours and kept on disk rather than on every 5-minute tick. A team plays
// at most once a night, so a few hours of staleness loses nothing.

const fs = require("fs");
const os = require("os");
const path = require("path");
const { cfg } = require("../../config");
const { q } = require("../../db");
const { seasonFloor } = require("./context");

const CACHE_PATH = path.join(os.tmpdir(), "bp-nba-ratings.json");
const STYLE_KEYS = ["par3", "ftr", "oreb", "tov"];

const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const possOf = (b) => b.fga + 0.44 * b.fta - b.oreb + b.tov;

/**
 * Pure. `games`: [{ game_id, kickoff, season, home_team, away_team, home_score,
 * away_score, neutral_site, box: { [team]: { fga, fg3a, fta, oreb, tov } } }].
 */
function computeRatings(games, asOf, rc) {
  const now = new Date(asOf).getTime();
  const usable = games.filter((g) => new Date(g.kickoff).getTime() < now
    && g.box && g.box[g.home_team] && g.box[g.away_team]
    && possOf(g.box[g.home_team]) > 0 && possOf(g.box[g.away_team]) > 0);
  if (!usable.length) return null;
  const current = Math.max(...usable.map((g) => Number(g.season)));

  const obs = {};   // team -> per-game observations
  let lgW = 0, lgPts = 0, lgPoss = 0;
  for (const g of usable) {
    const days = (now - new Date(g.kickoff).getTime()) / 86_400_000;
    const w = Number(g.season) === current
      ? Math.pow(0.5, days / rc.halfLifeDays)
      : rc.priorSeasonWeight;
    const poss = (possOf(g.box[g.home_team]) + possOf(g.box[g.away_team])) / 2;
    // Home court, in points per 100, split across the two rates so a team's
    // rating is what it would be on a neutral floor.
    const h = g.neutral_site ? 0 : (rc.homeCourtPts / poss) * 100 / 2;
    const add = (team, opp, ptsFor, ptsAgainst, adj) => {
      (obs[team] ||= []).push({
        w, opp, poss,
        off: (ptsFor / poss) * 100 - adj,
        def: (ptsAgainst / poss) * 100 + adj,
        me: g.box[team], them: g.box[opp],
      });
    };
    add(g.home_team, g.away_team, Number(g.home_score), Number(g.away_score), h);
    add(g.away_team, g.home_team, Number(g.away_score), Number(g.home_score), -h);
    lgW += 2 * w;
    lgPts += w * ((Number(g.home_score) + Number(g.away_score)) / poss) * 100;
    lgPoss += 2 * w * poss;
  }
  const lg = lgPts / lgW;
  const lgPace = lgPoss / lgW;
  const k = rc.shrinkGames;
  const teams = Object.keys(obs);

  // Opponent adjustment: a rating is what a team did, net of who it did it to.
  const off = Object.fromEntries(teams.map((t) => [t, lg]));
  const def = Object.fromEntries(teams.map((t) => [t, lg]));
  for (let it = 0; it < rc.iterations; it++) {
    const nOff = {}, nDef = {};
    for (const t of teams) {
      let w = 0, o = 0, d = 0;
      for (const x of obs[t]) {
        w += x.w;
        o += x.w * (x.off - (def[x.opp] - lg));
        d += x.w * (x.def - (off[x.opp] - lg));
      }
      nOff[t] = (o + k * lg) / (w + k);
      nDef[t] = (d + k * lg) / (w + k);
    }
    Object.assign(off, nOff);
    Object.assign(def, nDef);
  }

  // Style, for and allowed, then z-scored across the league.
  const rate = (b, poss, key) => (key === "par3" ? b.fg3a / b.fga
    : key === "ftr" ? b.fta / b.fga
    : key === "oreb" ? (b.oreb / poss) * 100
    : (b.tov / poss) * 100);
  const raw = {};
  for (const t of teams) {
    const r = { pace: 0, w: 0 };
    for (const key of STYLE_KEYS) { r[key] = 0; r[`${key}A`] = 0; }
    for (const x of obs[t]) {
      r.w += x.w;
      r.pace += x.w * x.poss;
      for (const key of STYLE_KEYS) {
        r[key] += x.w * rate(x.me, x.poss, key);
        r[`${key}A`] += x.w * rate(x.them, x.poss, key);
      }
    }
    r.pace = (r.pace + k * lgPace) / (r.w + k);
    for (const key of STYLE_KEYS) { r[key] /= r.w; r[`${key}A`] /= r.w; }
    raw[t] = r;
  }
  const zOf = (key) => {
    const v = teams.map((t) => raw[t][key]);
    const mean = v.reduce((a, b) => a + b, 0) / v.length;
    const sd = Math.sqrt(v.reduce((a, b) => a + (b - mean) ** 2, 0) / v.length) || 1;
    return (t) => (raw[t][key] - mean) / sd;
  };
  const z = {};
  for (const key of STYLE_KEYS) { z[key] = zOf(key); z[`${key}A`] = zOf(`${key}A`); }

  const out = {};
  for (const t of teams) {
    const currentGames = obs[t].filter((x) => x.w > rc.priorSeasonWeight).length;
    out[t] = {
      off: +off[t].toFixed(2), def: +def[t].toFixed(2), pace: +raw[t].pace.toFixed(2),
      currentGames,
      z: Object.fromEntries([...STYLE_KEYS, ...STYLE_KEYS.map((s) => `${s}A`)]
        .map((key) => [key, +z[key](t).toFixed(2)])),
    };
  }
  return { asOf, league: { off: +lg.toFixed(2), pace: +lgPace.toFixed(2) }, teams: out };
}

/** Ratings for this tick, from the disk cache when it is fresh enough. */
async function loadRatings(asOf) {
  const rc = cfg().model.ratings;
  try {
    const c = JSON.parse(fs.readFileSync(CACHE_PATH, "utf8"));
    const ageH = (new Date(asOf).getTime() - new Date(c.asOf).getTime()) / 3_600_000;
    if (ageH >= 0 && ageH < rc.cacheHours) return c;
  } catch { /* no cache yet */ }

  const { rows } = await q(
    `select g.game_id, g.kickoff, g.season, g.home_team, g.away_team, g.home_score, g.away_score,
            g.neutral_site, p.team, sum(p.fga)::int fga, sum(p.fg3a)::int fg3a, sum(p.fta)::int fta,
            sum(p.oreb)::int oreb, sum(p.tov)::int tov
       from sports.nba_games g
       join sports.nba_player_games p on p.game_id = g.game_id
      where g.home_score is not null and g.season >= $1 and g.season_type in (2, 5)
      group by g.game_id, p.team`,
    [seasonFloor(asOf)]);
  const byGame = new Map();
  for (const r of rows) {
    const g = byGame.get(r.game_id) || { ...r, box: {} };
    g.box[r.team] = { fga: r.fga, fg3a: r.fg3a, fta: r.fta, oreb: r.oreb, tov: r.tov };
    byGame.set(r.game_id, g);
  }
  const ratings = computeRatings([...byGame.values()], asOf, rc);
  if (ratings) {
    try { fs.writeFileSync(CACHE_PATH, JSON.stringify(ratings)); } catch { /* cache is optional */ }
  }
  return ratings;
}

/**
 * MATCHUP, as a disagreement: the margin the ratings expect, minus the margin the
 * price implies. Its weight is how far the model moves toward its own number, so
 * 0.45 means 45% of the way. This cannot be added to the market as a raw team
 * strength: the price already contains team strength, and that would double-count
 * it and simply back the better team every night.
 */
function matchupSignal(game, ctx, impliedMargin) {
  const mc = cfg().model.matchup;
  const r = ctx.ratings;
  const h = r && r.teams[game.home_team], a = r && r.teams[game.away_team];
  if (!h || !a) return { pts: 0, confidence: 0, detail: { reason: "no_ratings" } };

  const lg = r.league.off;
  const poss = (h.pace + a.pace) / 2;
  const homePts = ((h.off + a.def - lg) * poss) / 100;
  const awayPts = ((a.off + h.def - lg) * poss) / 100;
  const hca = game.neutral_site ? 0 : cfg().model.ratings.homeCourtPts;
  const margin = homePts - awayPts + hca;
  const gap = margin - impliedMargin;
  return {
    pts: clamp(gap, -mc.maxGapPts, mc.maxGapPts),
    confidence: clamp(Math.min(h.currentGames, a.currentGames) / mc.fullConfidenceGames, 0.4, 1),
    detail: {
      ratingMargin: +margin.toFixed(2), impliedMargin: +impliedMargin.toFixed(2), gap: +gap.toFixed(2),
      railed: Math.abs(gap) > mc.maxGapPts, poss: +poss.toFixed(1),
      home: { off: h.off, def: h.def, pace: h.pace, games: h.currentGames },
      away: { off: a.off, def: a.def, pace: a.pace, games: a.currentGames },
    },
  };
}

/**
 * STYLE MISMATCH: what team ratings cannot see. A rating already adds offense and
 * defense, so this is the INTERACTION: a turnover-prone offense meeting a defense
 * that forces them does worse than the two averages say. (z offense) x (z allowed)
 * per style, negative for turnovers because they hurt the team committing them.
 * Box-score style only: ESPN does not publish coverages or play types.
 */
function styleMismatchSignal(game, ctx) {
  const sc = cfg().model.style;
  const r = ctx.ratings;
  const h = r && r.teams[game.home_team], a = r && r.teams[game.away_team];
  if (!h || !a) return { pts: 0, confidence: 0, detail: { reason: "no_ratings" } };
  const parts = {};
  let pts = 0;
  for (const key of STYLE_KEYS) {
    const sign = key === "tov" ? -1 : 1;
    const v = sign * sc.ptsPerInteraction * (h.z[key] * a.z[`${key}A`] - a.z[key] * h.z[`${key}A`]);
    parts[key] = +v.toFixed(2);
    pts += v;
  }
  return {
    pts: clamp(pts, -sc.maxMismatchPts, sc.maxMismatchPts),
    confidence: clamp(Math.min(h.currentGames, a.currentGames) / sc.games, 0.4, 1),
    detail: parts,
  };
}

module.exports = { computeRatings, loadRatings, matchupSignal, styleMismatchSignal, STYLE_KEYS };
