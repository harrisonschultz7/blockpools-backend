// Unit checks for the pure v2 settlement mapping. Run: node scripts/settle-v2.verify.js
const { lockTimeOf, dateWindow, binaryVector, groupSubVector, matchWinnerIndex } = require("./settle-v2");

let fail = 0;
const eq = (name, got, want) => {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  if (!ok) fail++;
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}: got=${JSON.stringify(got)} want=${JSON.stringify(want)}`);
};

// lockTime from gameId trailing epoch
eq("lockTime from gameId", lockTimeOf({ gameId: "MLB-MIL-SF-2026-07-27-1785203100" }), 1785203100);
eq("lockTime explicit wins", lockTimeOf({ gameId: "X-1", lockTime: 999 }), 999);

// date window (exclusive end)
eq("dateWindow", dateWindow({ date: "2026-07-27" }), { dateFrom: "2026-07-27", dateTo: "2026-07-28" });
eq("dateWindow month rollover", dateWindow({ date: "2026-07-31" }), { dateFrom: "2026-07-31", dateTo: "2026-08-01" });
eq("dateWindow bad", dateWindow({ date: "nope" }), null);

// binary vectors
eq("binary A wins", binaryVector(0), [1, 0]);
eq("binary B wins", binaryVector(1), [0, 1]);
eq("binary void/draw", binaryVector(null), [1, 1]);

// match outcome -> winner index
eq("api A", matchWinnerIndex(0), 0);
eq("api B", matchWinnerIndex(1), 1);
eq("api draw", matchWinnerIndex(2), 2);
eq("api void", matchWinnerIndex(3), null);

// group sub-market: winner Yes, losers No (3-way: A=0,B=1,DRAW=2)
eq("group winner sub", groupSubVector(1, 1), [1, 0]); // sub is the winner
eq("group loser sub", groupSubVector(0, 1), [0, 1]);
eq("group draw sub wins", groupSubVector(2, 2), [1, 0]);
eq("group draw loser", groupSubVector(0, 2), [0, 1]);

console.log(fail === 0 ? "\nALL PASS" : `\n${fail} FAILURE(S)`);
process.exit(fail === 0 ? 0 : 1);
