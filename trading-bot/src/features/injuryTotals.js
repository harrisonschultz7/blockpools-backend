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
 * The two units are scaled SEPARATELY, and only because the data forced it.
 *
 * The original form was (defCost - offCost) x qbOutPoints: one constant, with
 * the sign carried by which unit was hurt. That bakes in "offensive damage
 * lowers the total" as an assumption, and measurement says it is wrong, or at
 * least not right. Games where the STARTING quarterback was ruled out or
 * doubtful went OVER 59.6% of the time against a 51.7% base (n=47, 2025+,
 * starters identified by top offensive snap share), averaging +2.18 points.
 *
 * The likely reason is that the market already over-corrects: books cut the
 * total hard on quarterback news and the game lands above the cut number. The
 * old form added another 2.5 points of cut on top of that.
 *
 * So offenceOutPoints is now small and POSITIVE -- offensive damage nudges the
 * total up, not down -- while defenceOutPoints keeps the original magnitude and
 * meaning, because the defensive half was never measured and inverting it on
 * the back of a quarterback finding would be inventing a result.
 *
 * Deliberately small: z is +1.08, which is under the |z| >= 2 bar this project
 * uses, so the honest position is "stop betting against it", not "bet on it".
 */
async function injuryTotalsSignal(game, asOf) {
  const m = cfg().model.injury;
  const [home, away] = await Promise.all([
    teamUnitPenalties(game.home_team, game.season, game.week, asOf),
    teamUnitPenalties(game.away_team, game.season, game.week, asOf),
  ]);

  const offCost = home.offense + away.offense;
  const defCost = home.defense + away.defense;
  // Fall back to the old single constant if the split keys are absent, so an
  // un-migrated config keeps its previous behaviour rather than silently
  // scaling everything to zero.
  const defPts = m.defenceOutPoints ?? m.qbOutPoints;
  const offPts = m.offenceOutPoints ?? -m.qbOutPoints;
  const raw = defCost * defPts + offCost * offPts;
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
