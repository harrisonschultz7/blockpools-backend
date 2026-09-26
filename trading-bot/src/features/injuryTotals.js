// trading-bot/src/features/injuryTotals.js
//
// INJURIES for a game total (weight 0.08). Output is in POINTS, positive meaning
// more points.
//
// THE SIGN DEPENDS ON THE UNIT, and this is the one structural difference from
// Adam-7's injury factor. For a moneyline, an injury is signed by TEAM: whoever
// is more damaged is worse off. For a total it is signed by UNIT -- an offensive
// player lost pushes the total DOWN, a defensive player lost pushes it UP. A
// model that summed both as "damage" would net a hurt quarterback against a hurt
// cornerback and conclude nothing happened.
//
// The BASELINING is reused wholesale from features/injuries.js, because that is
// the hard and valuable part: the penalty is the player's marginal value times
// one minus the demonstrated quality of whoever replaced him, shrunk toward
// positional replacement until the sample earns its way out. Without the
// shrinkage a team that played one competent game without its starter reads as
// "fine without him" on a sample of one.
//
// The weight is low at the user's direction -- injuries move a total less than
// they move a side. Worth noting the consequence plainly: at 0.08, a starting
// quarterback ruled out contributes about 0.2 points to the forecast before the
// residualisation and the cap touch it. That is a deliberate choice, not an
// oversight, and it is the first thing to revisit if the bot is seen taking overs
// in games where a backup is starting.

const { cfg } = require("../config");
const { currentReport, playerBaseline, canonPos } = require("./injuries");

// Which side of the ball a position plays. A kicker counts as offence: losing one
// costs points. Anything unrecognised is treated as offence, the conservative
// direction for a totals model, since it pulls the forecast toward the under
// rather than inventing scoring.
const DEFENSIVE = new Set(["EDGE", "DE", "DT", "NT", "LB", "ILB", "OLB", "CB", "S", "FS", "SS", "DB"]);
const isDefensive = (pos) => DEFENSIVE.has(canonPos(pos));

/** Points cost for one team, split by unit. Both numbers are non-negative. */
async function teamUnitPenalties(team, season, week, asOf) {
  const m = cfg().model.injury;
  const report = await currentReport(team, season, week, asOf);
  let offense = 0;
  let defense = 0;
  const items = [];

  for (const r of report.rows) {
    const base = await playerBaseline(team, r.full_name, asOf);
    if (!base) continue;
    const pos = canonPos(r.position);
    const posValue = m.positionValue[pos] ?? m.positionValue.default;
    const statusMult = m.statusMultiplier[r.report_status] ?? 0;
    if (statusMult === 0) continue;

    // Same shape as features/injuries.js: importance, discounted by how well the
    // replacement has actually performed.
    const importance = base.snapShare * posValue;
    const cost = importance * statusMult * (1 - base.replacementQuality);
    if (cost <= 0.0005) continue;

    if (isDefensive(pos)) defense += cost; else offense += cost;
    items.push({
      player: r.full_name, pos, unit: isDefensive(pos) ? "def" : "off",
      status: r.report_status,
      snapShare: +base.snapShare.toFixed(3),
      replacementQuality: +base.replacementQuality.toFixed(3),
      gamesWithout: base.gamesWithout,
      cost: +cost.toFixed(4),
    });
  }

  items.sort((a, b) => b.cost - a.cost);
  return { offense, defense, items, reportWeek: report.week, staleWeeks: report.staleWeeks };
}

/**
 * Injury signal for a game total, in points.
 *
 * Offensive damage on either team lowers the total; defensive damage on either
 * team raises it. qbOutPoints is the calibration anchor: a starting quarterback
 * with a full snap share, ruled Out, with a replacement of unknown quality,
 * produces a cost of 1.0 and therefore exactly that many points.
 */
async function injuryTotalsSignal(game, asOf) {
  const m = cfg().model.injury;
  const [home, away] = await Promise.all([
    teamUnitPenalties(game.home_team, game.season, game.week, asOf),
    teamUnitPenalties(game.away_team, game.season, game.week, asOf),
  ]);

  const offCost = home.offense + away.offense;
  const defCost = home.defense + away.defense;
  const raw = (defCost - offCost) * m.qbOutPoints;
  const points = Math.max(-m.maxPoints, Math.min(m.maxPoints, raw));

  // A week-old "Out" still carries real information -- most players ruled out
  // stay out -- but it is not Friday's report, and a MISSING report is not good
  // news. Same ladder as Adam-7.
  const stale = Math.max(home.staleWeeks ?? 9, away.staleWeeks ?? 9);
  const confidence = stale === 0 ? 1 : stale === 1 ? 0.6 : stale <= 2 ? 0.3 : 0.1;

  return {
    points,
    confidence,
    detail: {
      offenseCost: +offCost.toFixed(4),
      defenseCost: +defCost.toFixed(4),
      uncapped: +raw.toFixed(3),
      capped: Math.abs(raw) > m.maxPoints,
      reportWeek: home.reportWeek,
      staleWeeks: stale,
      home: home.items.slice(0, 5),
      away: away.items.slice(0, 5),
    },
  };
}

module.exports = { injuryTotalsSignal, teamUnitPenalties, isDefensive, DEFENSIVE };
