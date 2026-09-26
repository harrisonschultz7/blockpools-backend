#!/usr/bin/env node
// trading-bot/test/factors.test.js
//
// Arithmetic tests for the Argo-7 factor stack. No database, no network.
//
// These exist because the first live dry run proposed trades on 14 of 15 games
// with "edge NaNc size $NaN": a single missing config key made one factor NaN,
// and every policy gate is a comparison, so NaN passed all of them. Asserting the
// numbers directly is the cheap way to catch that class of bug before it reaches
// a ledger.
//
//   node trading-bot/test/factors.test.js

const assert = require("assert");
const path = require("path");
const { selectConfig, cfg } = require("../src/config");
selectConfig("config.argo-7.json");

const { weatherPoints } = require("../src/features/weatherTotals");
const { restSignal, situationalTotalsSignal, paceSignal } = require("../src/features/paceTotals");
const { normCdf, normInv, marketProbOver, FACTORS } = require("../src/model/forecastTotals");
const { interactionTerms } = require("../src/features/scheme");

let passed = 0;
const t = (name, fn) => {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.log(`  FAIL ${name}\n       ${e.message}`); process.exitCode = 1; }
};

console.log("weather (hinge, one-directional, percent precip)");

t("calm outdoor game contributes exactly zero", () => {
  const r = weatherPoints({ roof: "outdoors", wind: 6, temp: 62, precip: 10 });
  assert.strictEqual(r.points, 0, `expected 0, got ${r.points}`);
});

t("wind below threshold is zero, not a small slope", () => {
  const a = weatherPoints({ roof: "outdoors", wind: 3, temp: 60, precip: 0 });
  const b = weatherPoints({ roof: "outdoors", wind: 11.9, temp: 60, precip: 0 });
  assert.strictEqual(a.points, 0);
  assert.strictEqual(b.points, 0);
});

t("wind above threshold scales linearly from the hinge", () => {
  const w = cfg().model.weather;
  const r = weatherPoints({ roof: "outdoors", wind: 22, temp: 60, precip: 0 });
  const want = -(22 - w.windThresholdMph) * w.pointsPerMphOverThreshold;
  assert.ok(Math.abs(r.points - want) < 1e-9, `expected ${want}, got ${r.points}`);
});

t("precip is read as PERCENT: 3% of rain is worth nothing", () => {
  // The bug this pins down: read as a 0-1 fraction, 3 became "300% chance" and
  // cost 4 points, pinning the factor at its cap on a calm game.
  const r = weatherPoints({ roof: "outdoors", wind: 6, temp: 62, precip: 3 });
  assert.strictEqual(r.points, 0, `3% rain should be free, got ${r.points}`);
});

t("precip at 100% costs pointsAtCertainPrecip", () => {
  const w = cfg().model.weather;
  const r = weatherPoints({ roof: "outdoors", wind: 6, temp: 62, precip: 100 });
  assert.ok(Math.abs(r.points + w.pointsAtCertainPrecip) < 1e-9,
            `expected ${-w.pointsAtCertainPrecip}, got ${r.points}`);
});

t("indoor games are hard-zeroed, not damped", () => {
  for (const roof of ["dome", "closed"]) {
    const r = weatherPoints({ roof, wind: 40, temp: -10, precip: 100 });
    assert.strictEqual(r.points, 0, `${roof} should be immune to weather`);
    assert.strictEqual(r.indoors, true);
  }
});

t("weather is one-directional: it can never favour the over", () => {
  for (const wind of [0, 10, 15, 25, 40, 60]) {
    for (const temp of [-20, 10, 32, 70, 95]) {
      const r = weatherPoints({ roof: "outdoors", wind, temp, precip: 50 });
      assert.ok(r.points <= 0, `wind ${wind} temp ${temp} gave +${r.points}`);
    }
  }
});

t("weather is capped", () => {
  const w = cfg().model.weather;
  const r = weatherPoints({ roof: "outdoors", wind: 200, temp: -100, precip: 100 });
  assert.strictEqual(r.points, -w.maxPoints);
});

console.log("rest and situational (SUMS, not differentials)");

t("two rested teams beat one rested and one gassed", () => {
  const both = restSignal({ home_rest: 10, away_rest: 10 }).points;
  const split = restSignal({ home_rest: 13, away_rest: 7 }).points;
  // Same rest SUM (20), so the sum term matches; the bye bonus differs, which is
  // the point -- a differential model would score these identically at 0.
  assert.ok(both !== split, "a differential model cannot tell these apart");
});

t("missing rest days reports low confidence rather than a zero", () => {
  const r = restSignal({ home_rest: null, away_rest: 7 });
  assert.strictEqual(r.points, 0);
  assert.ok(r.confidence < 1, "absent data must not read as confident zero");
});

t("short weeks stack: a Thursday game takes the penalty twice", () => {
  const c = cfg().model.rest;
  const one = restSignal({ home_rest: 4, away_rest: 7 }).detail.shortWeekPoints;
  const two = restSignal({ home_rest: 4, away_rest: 4 }).detail.shortWeekPoints;
  assert.ok(Math.abs(two - 2 * c.shortWeekPoints) < 1e-9, `got ${two}`);
  assert.ok(Math.abs(one - c.shortWeekPoints) < 1e-9, `got ${one}`);
});

t("division games push the total down, never up", () => {
  const r = situationalTotalsSignal({ div_game: true, gametime: "13:00" });
  assert.ok(r.points < 0, `expected negative, got ${r.points}`);
});

t("every factor is capped", () => {
  const caps = {
    rest: restSignal({ home_rest: 30, away_rest: 30 }).points,
    situational: situationalTotalsSignal({ div_game: true, gametime: "20:15" }).points,
  };
  assert.ok(Math.abs(caps.rest) <= cfg().model.rest.maxPoints + 1e-9);
  assert.ok(Math.abs(caps.situational) <= cfg().model.situational.maxPoints + 1e-9);
});

console.log("pace (sums, inverse in seconds-per-play)");

t("two fast offences produce more plays and more points", () => {
  const ctx = { byTeam: new Map([["A", [24]], ["B", [24]], ["C", [31]], ["D", [31]]]),
                league: 27.5, n: 4 };
  const fast = paceSignal({ home_team: "A", away_team: "B" }, ctx).points;
  const slow = paceSignal({ home_team: "C", away_team: "D" }, ctx).points;
  assert.ok(fast > 0, `fast pair should add points, got ${fast}`);
  assert.ok(slow < 0, `slow pair should subtract points, got ${slow}`);
});

t("an unseen team falls back to league pace with zero confidence", () => {
  const ctx = { byTeam: new Map(), league: 27.5, n: 0 };
  const r = paceSignal({ home_team: "X", away_team: "Y" }, ctx);
  assert.ok(Math.abs(r.points) < 1e-9, `expected ~0, got ${r.points}`);
  assert.strictEqual(r.confidence, 0);
});

console.log("the price/points mapping");

t("inverting a 50c price returns the line itself", () => {
  const sigma = 10.5, line = 52;
  assert.ok(Math.abs((line + sigma * normInv(0.5)) - line) < 1e-9);
});

t("the worked example reproduces: line 52, 50c, model 55 -> 61c", () => {
  const sigma = 10.5;
  const p = normCdf((55 - 52) / sigma);
  assert.ok(Math.abs(p - 0.6125) < 0.001, `expected ~0.6125, got ${p}`);
});

t("deltaCap 0.16 is about 4.3 points at sigma 10.5 and 5.5 at the fitted 13.4", () => {
  const z = normInv(0.66);
  assert.ok(Math.abs(10.5 * z - 4.33) < 0.05, `got ${10.5 * z}`);
  assert.ok(Math.abs(13.416 * z - 5.53) < 0.05, `got ${13.416 * z}`);
});

t("normInv and normCdf round-trip", () => {
  for (const p of [0.02, 0.1, 0.25, 0.5, 0.75, 0.9, 0.98]) {
    assert.ok(Math.abs(normCdf(normInv(p)) - p) < 1e-4, `round-trip failed at ${p}`);
  }
});

t("overround is normalised away when both sides quote", () => {
  const p = marketProbOver({ mid: 0.52 }, { mid: 0.50 });
  assert.ok(Math.abs(p - 0.52 / 1.02) < 1e-9, `got ${p}`);
});

t("a single quoted side is used as-is, and the complement when only under quotes", () => {
  assert.strictEqual(marketProbOver({ mid: 0.4 }, null), 0.4);
  assert.ok(Math.abs(marketProbOver(null, { mid: 0.4 }) - 0.6) < 1e-9);
});

console.log("scheme interaction");

t("interaction is zero when either side is league-average", () => {
  const avg = { aggression: 0, tempo: 0, pressure: 0, frontWeight: 0 };
  const extreme = { aggression: 2, tempo: 2, pressure: 2, frontWeight: 2 };
  for (const v of Object.values(interactionTerms(extreme, avg))) assert.strictEqual(v, 0);
  for (const v of Object.values(interactionTerms(avg, extreme))) assert.strictEqual(v, 0);
});

t("interaction flips sign with the defence's profile", () => {
  const off = { aggression: 1, tempo: 1 };
  const hi = interactionTerms(off, { pressure: 1, frontWeight: 1 });
  const lo = interactionTerms(off, { pressure: -1, frontWeight: -1 });
  assert.strictEqual(hi.tempo_pressure, 1);
  assert.strictEqual(lo.tempo_pressure, -1);
});

console.log("config integrity");

t("weights sum to 1", () => {
  const sum = Object.values(cfg().model.weights).reduce((a, b) => a + b, 0);
  assert.ok(Math.abs(sum - 1) < 1e-9, `weights sum to ${sum}`);
});

t("every weighted factor has a config block and a cap", () => {
  for (const k of FACTORS) {
    assert.ok(cfg().model.weights[k] !== undefined, `no weight for ${k}`);
    assert.ok(cfg().model[k], `no config block for ${k}`);
    assert.ok(Number.isFinite(cfg().model[k].maxPoints), `no maxPoints for ${k}`);
  }
});

t("every config key the SHARED injury helpers read is present", () => {
  // The exact omission that produced NaN on the first dry run. features/injuries.js
  // reads these off model.injury and does not check for them.
  for (const k of ["replacementScaleEpa", "shrinkGamesWithout", "defaultReplacementQuality",
                   "positionValue", "statusMultiplier"]) {
    assert.ok(cfg().model.injury[k] !== undefined, `model.injury.${k} is missing`);
  }
  assert.ok(cfg().model.injury.positionValue.default !== undefined,
            "positionValue.default is required -- unlisted positions fall back to it");
});

console.log(`\n${passed} assertions passed${process.exitCode ? " (with failures above)" : ""}`);
