// Interim v2 settlement bot (operator-signed).
//
// Determines each active v2 market's outcome via the SAME Goalserve endpoint the
// AMM CRE uses (/api/scores/settlement), then resolves the market on-chain with
// vault.reportPayouts (the operator wallet is the vault resolver). This is the
// pragmatic bridge while the decentralized CRE path (V2SettlementReceiver) is
// built — same off-DON winner determination the AMM already relies on.
//
// Market kinds handled (Goalserve is match-based):
//   • binary moneyline  → [1,0] A wins / [0,1] B wins / [1,1] draw|void refund
//   • group 3-way       → winner sub-market [1,0] (Yes), losers [0,1] (No)
//   PROP + league-winner are NOT match-resolvable here → skipped (flagged).
//
// After resolving, it clears the matcher book for the market (stale orders can't
// be sniped) and POSTs to the backend so realized-PnL CLAIM rows are written.
//
//   node scripts/settle-v2.js            # settle everything final now
//   node scripts/settle-v2.js --dry      # preview + on-chain staticCall, no send
//   node scripts/settle-v2.js --market MLB-MIL-SF-2026-07-27-1785203100
//   node scripts/settle-v2.js --loop 300 # re-check every 5 min
//
// Env (backend .env): PRIVATE_KEY (resolver), VAULT_ADDRESS, GAMES_JSON_PATH,
//   BACKEND_URL (http://127.0.0.1:8080), ARBITRUM_RPC_URL (falls back to public),
//   SETTLEMENT_API_URL (default https://api.blockpools.io), MATCHER_URL.

const fs = require("fs");
const path = require("path");
// Load the backend .env by absolute path so a scheduler (cron / systemd) can run
// this from any working directory. Reuses PRIVATE_KEY (the AMM settler wallet =
// the vault resolver), plus VAULT_ADDRESS/GAMES_JSON_PATH/BACKEND_URL.
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { ethers } = require("ethers");

const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const SETTLEMENT_API_URL = (process.env.SETTLEMENT_API_URL || "https://api.blockpools.io").replace(/\/+$/, "");
const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const BACKEND_URL = (process.env.BACKEND_URL || "").replace(/\/+$/, "");

const VAULT_ABI = [
  "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
  "function reportPayouts(bytes32 marketId, uint256[] nums)",
];

const OUTCOME_A = 0, OUTCOME_B = 1, OUTCOME_DRAW = 2, OUTCOME_VOID = 3;

// ---- flag email alerts (reuse the app's Resend config) --------------------
// Set SETTLE_ALERT_EMAIL (comma-sep) in the backend .env to get an email when a
// game is flagged (resolved void/draw, held for manual settlement). RESEND_API_KEY
// + RESEND_FROM_EMAIL are the same vars the welcome email already uses.
const ALERT_TO = (process.env.SETTLE_ALERT_EMAIL || "").trim();
const ALERT_STATE = path.join(__dirname, ".settle-alerts.json");
const ALERT_REPEAT_MS = Number(process.env.SETTLE_ALERT_REPEAT_HOURS || 12) * 3600 * 1000;

function loadAlerts() { try { return JSON.parse(fs.readFileSync(ALERT_STATE, "utf8")); } catch { return {}; } }
function saveAlerts(a) { try { fs.writeFileSync(ALERT_STATE, JSON.stringify(a, null, 2)); } catch {} }

/** Email the operator about game(s) flagged for manual settlement.
 *  No-op unless SETTLE_ALERT_EMAIL + RESEND_API_KEY are set. */
async function emailFlagged(flagged) {
  if (!ALERT_TO || !process.env.RESEND_API_KEY) return false;
  let Resend;
  try { ({ Resend } = require("resend")); } catch { return false; }
  const rows = flagged.map((f) =>
    `<li><b>${f.gameId}</b> (${f.teams})<br><code>node scripts/settle-v2.js --market ${f.gameId} --winner &lt;0|1|void&gt;</code><br><small>${f.msg}</small></li>`
  ).join("");
  try {
    const r = await new Resend(process.env.RESEND_API_KEY).emails.send({
      from: process.env.RESEND_FROM_EMAIL || "BlockPools <welcome@mail.blockpools.io>",
      to: ALERT_TO.split(",").map((s) => s.trim()).filter(Boolean),
      subject: `🚩 ${flagged.length} v2 game(s) need manual settlement`,
      html: `<p>The v2 settler flagged the following game(s) — they resolved to a void/draw and were <b>NOT</b> settled on-chain (a real winner voiding is almost always a data error). Verify each result, then settle manually:</p><ul>${rows}</ul>`,
    });
    console.log(`  📧 flag alert emailed to ${ALERT_TO}${r?.data?.id ? ` (${r.data.id})` : ""}`);
    return true;
  } catch (e) { console.warn(`  flag email failed: ${e.message}`); return false; }
}

// ---- pure helpers (unit-tested by settle-v2.verify.js) --------------------

/** kickoff epoch (lockTime) for the settlement window: explicit field, else the
 *  trailing numeric segment of the gameId (…-<epoch>). */
function lockTimeOf(g) {
  if (g.lockTime != null && Number(g.lockTime) > 0) return Number(g.lockTime);
  const m = String(g.gameId || "").match(/(\d{9,})$/);
  return m ? Number(m[1]) : 0;
}

/** [dateFrom, dateTo) window (dateTo exclusive) covering the game's date. */
function dateWindow(g) {
  const d = String(g.date || "").trim(); // "YYYY-MM-DD"
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const dt = new Date(d + "T00:00:00Z");
  dt.setUTCDate(dt.getUTCDate() + 1);
  const to = dt.toISOString().slice(0, 10);
  return { dateFrom: d, dateTo: to };
}

/**
 * Payout vector for one binary market given a resolved match outcome.
 *   winnerOutcomeIndex is the vault outcome (0 or 1) that should win, or null for
 *   an even void/refund.
 */
function binaryVector(winnerOutcomeIndex) {
  if (winnerOutcomeIndex === 0) return [1, 0];
  if (winnerOutcomeIndex === 1) return [0, 1];
  return [1, 1]; // void / draw with no slot → even refund
}

/**
 * For a GROUP sub-market (its own binary Yes/No market), decide its vector from
 * the match's winning outcomeIndex and this sub-market's parent outcomeIndex.
 *   winner sub-market → Yes wins [1,0]; every other → No wins [0,1].
 */
function groupSubVector(subOutcomeIndex, matchWinnerIndex) {
  return Number(subOutcomeIndex) === Number(matchWinnerIndex) ? [1, 0] : [0, 1];
}

/** Map the settlement API outcome (0=A,1=B,2=draw,3=void) → winner index or null. */
function matchWinnerIndex(apiOutcome) {
  if (apiOutcome === OUTCOME_A) return 0;
  if (apiOutcome === OUTCOME_B) return 1;
  if (apiOutcome === OUTCOME_DRAW) return 2; // meaningful only for 3-way groups
  return null; // void
}

module.exports = { lockTimeOf, dateWindow, binaryVector, groupSubVector, matchWinnerIndex };

// ---- runtime (only runs when invoked directly, not when required) ---------

const DRY = process.argv.includes("--dry");
const marketFilter = argValue("--market");
const loopSec = Number(argValue("--loop") || 0);
const winnerOverride = argValue("--winner"); // manual force-resolve: 0 | 1 | void (requires --market)

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function loadV2Games() {
  const raw = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));
  const out = [];
  for (const list of Object.values(raw)) {
    if (Array.isArray(list)) for (const g of list) if (g && g.v2) out.push(g);
  }
  return out;
}

async function settlementOutcome(g) {
  const win = dateWindow(g);
  if (!win) throw new Error(`bad date for ${g.gameId}`);
  const q = new URLSearchParams({
    league: g.league || "",
    dateFrom: win.dateFrom,
    dateTo: win.dateTo,
    teamAName: g.teamAName || g.teamACode || g.teamA || "",
    teamBName: g.teamBName || g.teamBCode || g.teamB || "",
    teamACode: g.teamACode || g.teamA || "",
    teamBCode: g.teamBCode || g.teamB || "",
    lockTime: String(lockTimeOf(g)),
  });
  const res = await fetch(`${SETTLEMENT_API_URL}/api/scores/settlement?${q}`);
  if (!res.ok) throw new Error(`settlement API HTTP ${res.status}`);
  return res.json(); // { found, isFinal, outcome, ... }
}

async function postJson(url, body) {
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return res.ok;
  } catch {
    return false;
  }
}

/** Build the list of on-chain reportPayouts calls for one games.json entry.
 *  `allowVoid` is true only for a manual --winner override; automatic runs must
 *  NEVER settle a void/draw (see the guard below). */
function planForGame(g, apiOutcome, allowVoid) {
  const kind =
    Array.isArray(g.outcomes) && g.outcomes.length >= 2
      ? "group"
      : String(g.marketType || "").toUpperCase() === "PROP"
        ? "prop"
        : "binary";

  if (kind === "prop") return { skip: "PROP not match-resolvable" };

  const winner = matchWinnerIndex(apiOutcome);

  if (kind === "binary") {
    if (!g.marketId) return { skip: "no marketId" };
    const winIdx = winner === 0 ? 0 : winner === 1 ? 1 : null;
    // GUARD: a moneyline that resolves to void/draw (apiOutcome 2 or 3) almost
    // always means the feed/team-matching failed to pick a winner (e.g. the
    // shared-"Sox" false ambiguity) — NOT a real void. Auto-reporting [1,1]
    // half-pays the actual winner. So we DON'T settle it: flag for a human, who
    // verifies and forces the correct result with --winner <0|1|void>.
    if (winIdx === null && !allowVoid) {
      return { flag: `apiOutcome=${apiOutcome} → no clear winner. NOT auto-settled. Verify, then: --market ${g.gameId} --winner <0|1|void>` };
    }
    return { calls: [{ marketId: g.marketId, vector: binaryVector(winIdx), label: `${g.teamACode}/${g.teamBCode}` }] };
  }

  // group (3-way / league-winner). League-winner isn't match-resolvable, but a
  // 3-way is: winner sub-market Yes, losers No.
  if (winner == null) {
    if (!allowVoid) return { flag: `apiOutcome=${apiOutcome} → no clear winner for group. NOT auto-settled. Verify, then --market ${g.gameId} --winner <idx|void>` };
    return { skip: "void/unknown outcome for group (override)" };
  }
  if (g.outcomes.length > 3) return { skip: "league-winner not match-resolvable" };
  const calls = g.outcomes
    .filter((o) => o.marketId)
    .map((o) => ({
      marketId: o.marketId,
      vector: groupSubVector(o.outcomeIndex, winner),
      label: o.code,
    }));
  return { calls };
}

async function main() {
  const RPC = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";
  const VAULT = process.env.VAULT_ADDRESS || process.env.V2_VAULT_ADDRESS;
  const KEY = process.env.PRIVATE_KEY || process.env.OPERATOR_PRIVATE_KEY;
  if (!VAULT || !KEY) throw new Error("Set VAULT_ADDRESS (or V2_VAULT_ADDRESS) and PRIVATE_KEY in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  const vault = new ethers.Contract(VAULT, VAULT_ABI, wallet);
  console.log(`settle-v2: vault=${VAULT} resolver=${wallet.address}${DRY ? " (DRY RUN)" : ""}`);

  if (winnerOverride != null && !marketFilter) {
    throw new Error("--winner requires --market <gameId> (refusing to force-resolve every v2 market)");
  }

  const tick = async () => {
    let games = loadV2Games();
    if (marketFilter) games = games.filter((g) => g.gameId === marketFilter || g.slug === marketFilter);
    if (!games.length) return console.log("no matching v2 markets");

    const flagged = [];
    for (const g of games) {
      try {
        let api;
        if (winnerOverride != null) {
          const ov = String(winnerOverride).toLowerCase() === "void" ? OUTCOME_VOID : Number(winnerOverride);
          api = { found: true, isFinal: true, outcome: ov };
          console.log(`  ${g.gameId}: MANUAL override → outcome=${ov} (0=A/${g.teamACode}, 1=B/${g.teamBCode}, void=refund)`);
        } else {
          try {
            api = await settlementOutcome(g);
          } catch (e) {
            console.log(`  ${g.gameId}: settlement lookup failed (${e.message}) — retry next pass`);
            continue;
          }
          if (!api.found || !api.isFinal || api.outcome == null) {
            console.log(`  ${g.gameId}: not final yet (found=${api.found} status="${api.status || ""}")`);
            continue;
          }
        }

        const plan = planForGame(g, Number(api.outcome), winnerOverride != null);
        if (plan.flag) {
          flagged.push({ gameId: g.gameId, teams: `${g.teamACode}/${g.teamBCode}`, msg: plan.flag });
          console.warn(`  🚩 FLAG ${g.gameId} (${g.teamACode}/${g.teamBCode}): ${plan.flag}`);
          continue;
        }
        if (plan.skip) {
          console.log(`  ${g.gameId}: skip — ${plan.skip}`);
          continue;
        }

        for (const call of plan.calls) {
          const mid = call.marketId;
          const m = await vault.markets(mid);
          if (!m.exists) {
            console.log(`  ${g.gameId}/${call.label}: market ${mid} not found on-chain — skip`);
            continue;
          }
          if (m.resolved) {
            console.log(`  ${g.gameId}/${call.label}: already resolved — skip`);
            continue;
          }
          const nums = call.vector.map((n) => BigInt(n));
          // Always simulate first so a bad call never wastes gas / mis-settles.
          await vault.reportPayouts.staticCall(mid, nums);
          if (DRY) {
            console.log(`  ${g.gameId}/${call.label}: WOULD reportPayouts([${call.vector}]) ✓ (staticCall ok)`);
            continue;
          }
          const tx = await vault.reportPayouts(mid, nums);
          await tx.wait();
          console.log(`  ${g.gameId}/${call.label}: resolved [${call.vector}] tx=${tx.hash}`);
          // Clear the matcher book so stale resting orders can't be sniped.
          await postJson(`${MATCHER_URL}/clear-market`, { marketId: mid });
          // Tell the backend to write realized-PnL CLAIM rows for winners.
          if (BACKEND_URL) {
            await postJson(`${BACKEND_URL}/api/v2/resolve`, {
              marketId: mid,
              winningOutcome: call.vector[0] === call.vector[1] ? null : call.vector.indexOf(1),
              gameId: g.gameId,
            });
          }
        }
      } catch (e) {
        console.log(`  ${g.gameId}: ERROR ${e.shortMessage || e.message}`);
      }
    }

    if (flagged.length) {
      console.warn(`\n🚩🚩 ${flagged.length} GAME(S) FLAGGED — resolved to void/draw and NOT settled on-chain. Review each, then settle manually:`);
      for (const f of flagged) console.warn(`   ${f.gameId} (${f.teams})  →  node scripts/settle-v2.js --market ${f.gameId} --winner <0|1|void>`);

      // Email the operator — only on the unattended full sweep (a manual --market
      // run is interactive), de-duped so a persistently-flagged game re-alerts at
      // most every SETTLE_ALERT_REPEAT_HOURS instead of every pass.
      if (!marketFilter) {
        const alerts = loadAlerts();
        const now = Date.now();
        const fresh = flagged.filter((f) => !alerts[f.gameId] || now - alerts[f.gameId] > ALERT_REPEAT_MS);
        if (fresh.length && (await emailFlagged(fresh))) for (const f of fresh) alerts[f.gameId] = now;
        const still = new Set(flagged.map((f) => f.gameId)); // prune settled games so a recurrence re-alerts
        for (const gid of Object.keys(alerts)) if (!still.has(gid)) delete alerts[gid];
        saveAlerts(alerts);
      }
    }
  };

  // Crash-safe wrapper: a thrown tick must never kill an unattended loop.
  const safeTick = async () => {
    console.log(`[${new Date().toISOString()}] settle-v2 pass…`);
    try {
      await tick();
    } catch (e) {
      console.error(`[${new Date().toISOString()}] tick error:`, e.shortMessage || e.message);
    }
  };

  await safeTick();
  if (loopSec > 0) {
    console.log(`looping every ${loopSec}s…`);
    setInterval(safeTick, loopSec * 1000);
  }
}

if (require.main === module) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
