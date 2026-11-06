#!/usr/bin/env node
// trading-bot/test/nba.test.js
//
// The NBA injury inputs. No database, no network.
//
//   node trading-bot/test/nba.test.js

const assert = require("assert");
const { selectConfig, cfg } = require("../src/config");
selectConfig("config.axis-3.json");

const { statusFromNote } = require("../src/ingest/espnNba");
const { playerImpact } = require("../src/features/nba/factors");

let passed = 0;
const t = (name, fn) => {
  fn();
  passed++;
  console.log(`  ok  ${name}`);
};

// A Thursday evening, US-Eastern.
const THU = new Date("2026-11-05T23:00:00Z");
// Just after midnight UTC, still Thursday in the East.
const THU_LATE = new Date("2026-11-06T02:00:00Z");

t("a note ruling him out today makes GTD OUT", () => {
  assert.strictEqual(statusFromNote("GTD", "Goodwin (calf) is out for Thursday's game against Orlando.", THU), "OUT");
  assert.strictEqual(statusFromNote("GTD", "Joe (hip) has been ruled out for Thursday's game.", THU), "OUT");
  assert.strictEqual(statusFromNote("GTD", "Smith will not play Thursday against Boston.", THU), "OUT");
  assert.strictEqual(statusFromNote("GTD", "Smith is out tonight.", THU), "OUT");
});
t("today is the Eastern day, not the UTC one", () => {
  assert.strictEqual(statusFromNote("GTD", "Goodwin is out for Thursday's game.", THU_LATE), "OUT");
});
t("a note naming another day leaves the status alone", () => {
  assert.strictEqual(statusFromNote("GTD", "DeRozan has been ruled out for Wednesday's game.", THU), "GTD");
  assert.strictEqual(statusFromNote("GTD", "Santos (knee) is questionable for Saturday's game.", THU), "GTD");
});
t("no note, or a status already out, is unchanged", () => {
  assert.strictEqual(statusFromNote("GTD", null, THU), "GTD");
  assert.strictEqual(statusFromNote("OFS", "He is out for Thursday's game.", THU), "OFS");
});

const ic = () => cfg().model.injury;
const base = { min_sum: 34, pm_sum: 7, n_without: 29, margin_with: 6, margin_without: 3 };
t("one game cannot make a player worth much", () => {
  const one = playerImpact({ ...base, gp: 1 }, 2, ic());
  assert.ok(Number.isFinite(one), "finite");
  assert.ok(one < 1.5, `one game: ${one}`);
});
t("a full sample is not scaled", () => {
  const n = ic().minImpactGames;
  const p = { gp: n, min_sum: n * 34, pm_sum: n * 7, n_without: 0, margin_with: 6, margin_without: null };
  const more = { ...p, gp: n * 2, min_sum: n * 2 * 34, pm_sum: n * 2 * 7 };
  assert.ok(playerImpact(p, 2, ic()) > 3, "a real starter keeps his value");
  assert.ok(Math.abs(playerImpact(p, 2, ic()) - playerImpact(more, 2, ic())) < 1.5, "more games barely moves it");
});
t("a config without the key cannot produce NaN", () => {
  const { minImpactGames, ...old } = ic();
  assert.ok(Number.isFinite(playerImpact({ ...base, gp: 1 }, 2, old)));
});

console.log(`\n${passed} passed`);
