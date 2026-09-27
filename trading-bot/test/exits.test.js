// trading-bot/test/exits.test.js
//
// The resting-sell fill convention. Locked down by test because the failure is
// silent and flattering: an exit credited with the bid rather than its own limit
// turns every gap in the book recorder into paper profit, and nothing in the
// logs looks wrong.

const test = require("node:test");
const assert = require("node:assert");

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");
const { walkBids } = require("../src/exec/limits");

test("a resting sell is paid its limit, never the bid", () => {
  // The NE @ JAX shape: limit 0.670, bid ran to 0.740 while the recorder was
  // not polling our line.
  const bids = [[0.74, 312], [0.73, 1449], [0.72, 33110], [0.70, 15150]];
  const hit = walkBids(bids, 327, 0.67);

  assert.equal(hit.filledShares, 327, "deep bid above the limit fills in full");
  assert.ok(Math.abs(hit.avgPrice - 0.67) < 1e-9,
    `paid ${hit.avgPrice}, expected the 0.670 limit`);
  assert.ok(Math.abs(hit.proceedsUsd - 327 * 0.67) < 1e-6);
});

test("bids below the limit do not fill", () => {
  const hit = walkBids([[0.60, 5000]], 327, 0.67);
  assert.equal(hit.filledShares, 0);
  assert.equal(hit.avgPrice, null);
  assert.equal(hit.unfilledShares, 327);
});

test("thin depth fills partially, still at the limit", () => {
  const hit = walkBids([[0.68, 100], [0.66, 9999]], 327, 0.67);
  assert.equal(hit.filledShares, 100, "stops at the level below the limit");
  assert.equal(hit.unfilledShares, 227);
  assert.ok(Math.abs(hit.avgPrice - 0.67) < 1e-9);
});

test("depth is reported honestly even though we are not paid for it", () => {
  const hit = walkBids([[0.74, 200], [0.72, 200]], 327, 0.67);
  // consumed shows the real levels that were standing above our limit.
  assert.deepEqual(hit.consumed.map((c) => c[0]), [0.74, 0.72]);
  assert.ok(Math.abs(hit.avgPrice - 0.67) < 1e-9, "but the price is still ours");
});
