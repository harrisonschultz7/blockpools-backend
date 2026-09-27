// trading-bot/src/model/fitAbsolute.js
//
// Fits the INDEPENDENT mode's base total: how many points a team scores, given
// its own offensive quality, its opponent's defensive quality, and the expected
// number of plays.
//
//   points_for ~ b0 + b_off*myOffRating + b_def*oppDefRating + b_plays*expPlays
//
// Writes model/absolute-fit.json.
//
// WHAT THIS IS AND IS NOT. This is the structural core of the independent price
// -- the part that says a Bills-Chiefs game is worth more points than a
// Giants-Panthers game. It carries NO user weight, because it is not a factor
// competing with weather or rest; it is the price level those factors then
// adjust. Scheme STYLE is deliberately excluded and lives in the scheme factor
// at its own weight, so the two cannot count the same thing twice.
//
// OFFENCE AND DEFENCE ARE THE BIGGEST EFFECTS IN THE MODEL, by a wide margin:
// t 13.1 and 6.6 against points scored, where every style term is below 2.4.
// That matches the intuition this bot was specified on.
//
// THE COST, recorded here because it is the single most important number in this
// file: out of sample the implied game-total error is about 13.5 points, against
// the Polymarket closing line's 13.07. The base model is a genuine football model
// and it is still slightly WORSE than the price it trades against. Independent
// mode was chosen with that on the table. sigma is therefore taken from THIS
// model's own out-of-sample residual, never from the line's, so the forecast is
// appropriately less confident and Kelly sizes down accordingly.
//
// POINT-IN-TIME: ratings are solved as of each game's kickoff, walking the
// calendar week by week. Solving once over the full history would let a team's
// end-of-season rating predict its own week-2 game.

const fs = require("fs");
const path = require("path");
const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");
const { wls } = require("./fitScheme");
const { solveRatings } = require("../features/teamStrength");

const FIT_PATH = path.join(__dirname, "absolute-fit.json");
// expPlays is GONE, and its removal is the most important line in this file.
//
// It fitted beautifully -- t 11.1 against points scored -- because a game with
// more snaps mechanically produces more points. But the fit used the REALISED
// play count, which the live bot does not have. At forecast time it has to be
// substituted with a pace estimate, and that estimate was measured against 318
// games: error SD 9.9 plays, versus 9.9 plays for simply guessing the league
// average. It carries no information whatsoever. A team's own recent play-count
// history is no better (9.23 vs 9.33).
//
// Two consequences, both bad, both now fixed by removal:
//  1. The term injected 0.464 pts of total per play of estimate error, which on
//     the 2026 week-3 slate was 24% of the average disagreement with the market
//     -- a quarter of the bot's apparent edge was noise it had manufactured.
//  2. It made the reported accuracy a LIE. The holdout sigma of 13.075 pts was
//     computed using realised plays, i.e. with knowledge the live model cannot
//     have, so it flattered the model against a closing line that had no such
//     help. Fitting only on what is forecastable makes the number honest.
//
// Snap tempo is not play volume. Total plays depend on drive count, turnovers,
// penalties and clock management far more than on how fast a team snaps, which is
// why sec_per_play predicts it no better than a constant does.
const COLS = ["intercept", "myOffRating", "oppDefRating"];

/** One observation per (finished game, team). */
async function buildSamples() {
  const c = cfg();
  const first = c.model.absolute.fitStartSeason || c.data.startSeason;
  const minGames = c.model.absolute.minGamesToPrice;

  const { rows } = await q(
    `select g.game_id, g.season, g.week, g.kickoff,
            s.team, s.opponent, s.points_for
       from sports.nfl_games g
       join sports.nfl_team_game_stats s on s.game_id = g.game_id
      where g.season >= $1
        and g.home_score is not null
        and s.points_for is not null
      order by g.kickoff`,
    [first],
  );

  const samples = [];
  let key = null;
  let ratings = null;
  for (const r of rows) {
    const k = `${r.season}-${r.week}`;
    if (k !== key) {
      ratings = await solveRatings(new Date(r.kickoff).toISOString(), { fromSeason: first });
      key = k;
    }
    const me = ratings.get(r.team);
    const opp = ratings.get(r.opponent);
    if (!me || !opp) continue;
    if (me.games < minGames || opp.games < minGames) continue;
    // Only forecastable inputs. See the COLS comment: the realised play count was
    // in here and had to come out, because a model may not be fitted on a variable
    // it will not have at prediction time.
    samples.push({
      season: r.season, week: r.week, game_id: r.game_id, team: r.team,
      y: Number(r.points_for),
      x: [1, me.off, opp.def],
    });
  }
  return samples;
}

/**
 * Fit, and measure sigma OUT OF SAMPLE.
 *
 * The split is by season, not random: a random split leaks, because two teams in
 * the same game share a rating context and one half would be predicting the other.
 * Holding out whole later seasons is the only honest version, and it is also the
 * question that matters -- does a model fitted on old seasons work on new ones.
 */
async function fitAbsolute({ write = true } = {}) {
  const ac = cfg().model.absolute;
  const samples = await buildSamples();
  log(`absolute fit: ${samples.length} team-game observations`);
  if (samples.length < 400) {
    log.warn(`absolute fit: ${samples.length} observations is too few. Writing nothing.`);
    return { fitted: false, n: samples.length };
  }

  const seasons = [...new Set(samples.map((s) => s.season))].sort();
  const holdout = seasons.slice(-2);
  const train = samples.filter((s) => !holdout.includes(s.season));
  const test = samples.filter((s) => holdout.includes(s.season));
  // An empty split means the ratings lookback did not reach the early seasons, so
  // every sample landed in the holdout. That crashed inside wls() with an opaque
  // "cannot read length of undefined" rather than saying what was wrong.
  if (!train.length || !test.length) {
    throw new Error(
      `absolute fit: season split is empty (train ${train.length}, test ${test.length}). ` +
      `Seasons present: ${seasons.join(",")}. Check model.absolute.fitStartSeason and that ` +
      `run/backfill-history.js has populated nfl_team_game_stats that far back.`);
  }

  const ones = (a) => a.map(() => 1);
  const oos = wls(train.map((s) => s.x), train.map((s) => s.y), ones(train), 0.001);
  let rss = 0, tss = 0;
  const ymean = test.reduce((a, s) => a + s.y, 0) / test.length;
  for (const s of test) {
    const pred = s.x.reduce((t, v, j) => t + v * oos.beta[j], 0);
    rss += (s.y - pred) ** 2;
    tss += (s.y - ymean) ** 2;
  }
  const teamSigmaOos = Math.sqrt(rss / test.length);
  const r2Oos = 1 - rss / tss;

  // The coefficients that ship are fitted on EVERYTHING -- there is no reason to
  // throw away two seasons once the honest error estimate has been taken from the
  // holdout.
  const full = wls(samples.map((s) => s.x), samples.map((s) => s.y), ones(samples), 0.001);

  // A game total is two teams. Their errors are not independent -- a shootout
  // inflates both -- so the game sigma is measured directly from summed game
  // residuals rather than assumed to be sqrt(2) times the team sigma.
  const byGame = new Map();
  for (const s of test) {
    const pred = s.x.reduce((t, v, j) => t + v * oos.beta[j], 0);
    if (!byGame.has(s.game_id)) byGame.set(s.game_id, { actual: 0, pred: 0, n: 0 });
    const g = byGame.get(s.game_id);
    g.actual += s.y; g.pred += pred; g.n++;
  }
  const gameErrs = [...byGame.values()].filter((g) => g.n === 2).map((g) => g.actual - g.pred);
  const gMean = gameErrs.reduce((a, b) => a + b, 0) / gameErrs.length;
  const gameSigmaOos = Math.sqrt(
    gameErrs.reduce((a, e) => a + (e - gMean) ** 2, 0) / (gameErrs.length - 1));

  const named = {};
  COLS.forEach((k, i) => {
    named[k] = { beta: +full.beta[i].toFixed(5), se: +full.se[i].toFixed(5), t: +full.t[i].toFixed(2) };
  });

  const out = {
    fittedAt: new Date().toISOString(),
    n: samples.length,
    seasons,
    holdoutSeasons: holdout,
    coef: Object.fromEntries(COLS.map((k, i) => [k, +full.beta[i].toFixed(5)])),
    diagnostics: named,
    r2InSample: +full.r2.toFixed(4),
    r2OutOfSample: +r2Oos.toFixed(4),
    teamSigmaOutOfSample: +teamSigmaOos.toFixed(3),
    // THE number that governs sizing. Compare it against the closing line's own
    // error (calibration-totals.json sigma) before trusting a disagreement.
    gameSigmaOutOfSample: +gameSigmaOos.toFixed(3),
    gamesInHoldout: gameErrs.length,
    meanGameBias: +gMean.toFixed(3),
  };

  if (write) fs.writeFileSync(FIT_PATH, JSON.stringify(out, null, 2) + "\n");
  log(`absolute fit: r2 in-sample ${out.r2InSample}, out-of-sample ${out.r2OutOfSample}`);
  log(`  team sigma ${out.teamSigmaOutOfSample} pts, GAME sigma ${out.gameSigmaOutOfSample} pts ` +
      `over ${out.gamesInHoldout} holdout games`);
  log(`  mean game bias ${out.meanGameBias} pts (a large value here means the model ` +
      `is systematically high or low, which in independent mode becomes a standing bet)`);
  for (const k of COLS) {
    log(`  ${k.padEnd(14)} beta ${String(named[k].beta).padStart(10)}  t ${String(named[k].t).padStart(7)}`);
  }
  return out;
}

/** Fitted coefficients, or an explicit refusal. */
function loadAbsoluteFit() {
  try {
    const f = JSON.parse(fs.readFileSync(FIT_PATH, "utf8"));
    return { ...f, fitted: true };
  } catch {
    return { fitted: false };
  }
}

module.exports = { fitAbsolute, buildSamples, loadAbsoluteFit, FIT_PATH, COLS };
