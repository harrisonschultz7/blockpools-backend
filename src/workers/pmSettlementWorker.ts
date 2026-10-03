// ---------------------------------------------------------------------------
// pm settlement worker
//
// When a Polymarket market resolves, its positions in our ledger stay "Pending"
// forever (nothing writes a resolution/claim back to Supabase) — so wins never
// roll into Trade History or realized P&L. This worker closes that gap:
//
//   1. Find open pm GAME markets past lock (games.is_final != true).
//   2. Look up the matching RESOLVED event on Polymarket Gamma (closed=true,
//      umaResolutionStatus=resolved, a clean 1/0 outcome).
//   3. Write the resolution onto public.games (is_final + winning_outcome_index
//      + winner_side + winner_team_code + resolution_type='RESOLVED'). This
//      alone flips every position Won/Lost and realizes losses in the ROI view.
//   4. Write a CLAIM row (payout = winning shares x $1) for each winner so the
//      Return/ROI column and leaderboard realized P&L are correct (without it a
//      win shows -100% because returnAmount = sell + claim).
//
// Supports BINARY (NFL/MLB/NBA/NHL — two team outcomes) and SOCCER 3-WAY
// (EPL/UCL — Home / Draw / Away). Our outcome-index convention:
//   binary: 0 = team A (home), 1 = team B (away)
//   soccer: 0 = team A (home), 1 = DRAW, 2 = team B (away)   [matches tradeAgg]
//
// SAFETY: only settle on an exact lock-time match; binary additionally requires
// the slug team-code pair to match ours, soccer matches on lock-time (+ a team
// name check when available). Clean 1/0 split only; winner must map to one of
// our stored outcomes. Idempotent (games guarded by is_final; CLAIM rows deduped
// by a deterministic id). Disable via PM_SETTLE_ENABLED=false.
// ---------------------------------------------------------------------------
import { pool } from "../db";

const GAMMA = (process.env.GAMMA_API_URL || "https://gamma-api.polymarket.com").replace(/\/+$/, "");

// Two-outcome moneyline leagues.
const BINARY_TAGS: Record<string, number> = { NFL: 450, MLB: 100381, NBA: 745, NHL: 899 };
// Three-way (Home/Draw/Away) soccer leagues.
const SOCCER_TAGS: Record<string, number> = { EPL: 306, UCL: 100977 };
const TAGS: Record<string, number> = { ...BINARY_TAGS, ...SOCCER_TAGS };

const INTERVAL_MS = Number(process.env.PM_SETTLE_INTERVAL_MS || 15 * 60_000);

function parseArr(x: any): any[] {
  try {
    return Array.isArray(x) ? x : JSON.parse(x || "[]");
  } catch {
    return [];
  }
}

const normName = (s: string) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");

/** "nhl-bos-wpg-2026-10-02" / "epl-bou-liv-2026-09-20" -> ["BOS","WPG"] (home, away). */
function slugCodes(slug: string): [string, string] | null {
  const m = String(slug || "").match(/^[a-z]+-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d{2}-\d{2}/i);
  return m ? [m[1].toUpperCase(), m[2].toUpperCase()] : null;
}

type Winner = "HOME" | "AWAY" | "DRAW";
type Resolution = {
  lockTime: number;
  codes: [string, string]; // slug [home, away]
  homeName: string;
  awayName: string;
  winner: Winner;
};

async function fetchResolvedEvents(tagId: number): Promise<any[]> {
  const url = `${GAMMA}/events?closed=true&tag_id=${tagId}&limit=200&order=startDate&ascending=false`;
  try {
    const r = await fetch(url);
    if (!r.ok) return [];
    const d = await r.json();
    return Array.isArray(d) ? d : (d as any).events || [];
  } catch (e: any) {
    console.warn(`[pm-settle] gamma fetch failed (tag ${tagId}): ${e?.message || e}`);
    return [];
  }
}

function titleNames(ev: any): [string, string] {
  const [h, a] = String(ev?.title || "").split(/\s+vs\.?\s+/i);
  return [String(h || "").trim(), String(a || "").trim()];
}

/** Yes-price ~1 => this outcome won. Returns true/false/null(ambiguous). */
function yesWon(m: any): boolean | null {
  const prices = parseArr(m?.outcomePrices).map((p: any) => Number(p));
  if (prices.length !== 2) return null;
  if (prices[0] >= 0.99 && prices[1] <= 0.01) return true;
  if (prices[0] <= 0.01 && prices[1] >= 0.99) return false;
  return null; // void / mid / unresolved
}

/** Binary (two team outcomes) resolution. */
function readBinary(ev: any): Resolution | null {
  if (ev?.closed !== true) return null;
  const codes = slugCodes(ev.slug);
  if (!codes) return null;
  const mls = (ev.markets || []).filter((m: any) => String(m?.sportsMarketType || "") === "moneyline");
  const ml = mls[0] || (ev.markets || [])[0];
  if (!ml) return null;
  const uma = String(ml.umaResolutionStatus || "").toLowerCase();
  if (uma && uma !== "resolved") return null;
  const start = Date.parse(ml.gameStartTime || ev.startDate || "");
  if (!Number.isFinite(start)) return null;

  const prices = parseArr(ml.outcomePrices).map((p: any) => Number(p));
  const outcomes = parseArr(ml.outcomes);
  if (prices.length !== 2 || outcomes.length !== 2) return null;
  const hi = prices.findIndex((p) => p >= 0.99);
  const lo = prices.findIndex((p) => p <= 0.01);
  if (hi < 0 || lo < 0 || hi === lo) return null; // ambiguous/void -> skip

  const [homeName, awayName] = titleNames(ev);
  return {
    lockTime: Math.floor(start / 1000),
    codes,
    homeName,
    awayName,
    winner: hi === 0 ? "HOME" : "AWAY", // outcomes[0] is the home/A team
  };
}

/** Soccer 3-way (Home / Draw / Away) resolution. The event has three Yes/No
 *  moneyline sub-markets; the winner is the one whose Yes resolved to 1. */
function readSoccer(ev: any): Resolution | null {
  if (ev?.closed !== true) return null;
  const codes = slugCodes(ev.slug);
  if (!codes) return null;
  const subs = (ev.markets || []).filter((m: any) => String(m?.sportsMarketType || "") === "moneyline");
  if (subs.length < 3) return null;
  if (subs.some((m: any) => String(m.umaResolutionStatus || "").toLowerCase() !== "resolved")) return null;

  const start = Date.parse(subs[0].gameStartTime || ev.startDate || "");
  if (!Number.isFinite(start)) return null;

  // Exactly one sub-market must have Yes ~ 1.
  const won = subs.filter((m: any) => yesWon(m) === true);
  if (won.length !== 1) return null; // ambiguous / void / not fully resolved
  const w = won[0];
  const git = String(w.groupItemTitle || "");

  const [homeName, awayName] = titleNames(ev);
  let winner: Winner;
  if (/draw/i.test(git)) {
    winner = "DRAW";
  } else {
    const g = normName(git);
    const h = normName(homeName);
    const a = normName(awayName);
    const matchesHome = h && (g.includes(h) || h.includes(g));
    const matchesAway = a && (g.includes(a) || a.includes(g));
    if (matchesHome && !matchesAway) winner = "HOME";
    else if (matchesAway && !matchesHome) winner = "AWAY";
    else return null; // can't confidently classify -> skip
  }

  return { lockTime: Math.floor(start / 1000), codes, homeName, awayName, winner };
}

type OpenGame = {
  game_id: string;
  league: string;
  lock_time: number;
  team_a_code: string | null;
  team_b_code: string | null;
  team_a_name: string | null;
  team_b_name: string | null;
};

async function loadOpenGames(): Promise<OpenGame[]> {
  const nowSec = Math.floor(Date.now() / 1000);
  const res = await pool.query(
    `
    SELECT g.game_id, g.league, g.lock_time, g.team_a_code, g.team_b_code, g.team_a_name, g.team_b_name
    FROM public.games g
    WHERE g.game_id ILIKE '%-pm-%'
      AND COALESCE(g.is_final, false) = false
      AND g.lock_time IS NOT NULL
      AND g.lock_time < $1
      AND upper(g.league) = ANY($2)
    `,
    [nowSec, Object.keys(TAGS)],
  );
  return res.rows as OpenGame[];
}

const pairKey = (a?: string | null, b?: string | null) =>
  [String(a || "").toUpperCase(), String(b || "").toUpperCase()].sort().join("|");

/** Settle one matched game: write resolution + winner CLAIM rows. */
async function settleGame(
  g: OpenGame,
  winningIndex: 0 | 1 | 2,
  winnerCode: string, // team code, or 'DRAW'
  winnerSide: "A" | "B" | null,
): Promise<{ claimed: number; payoutTotal: number }> {
  const upd = await pool.query(
    `
    UPDATE public.games SET
      is_final = true,
      winning_outcome_index = $2,
      winner_side = $3,
      winner_team_code = $4,
      resolution_type = 'RESOLVED',
      updated_at = now()
    WHERE game_id = $1 AND COALESCE(is_final, false) = false
    `,
    [g.game_id, winningIndex, winnerSide, winnerCode],
  );
  if (upd.rowCount === 0) return { claimed: 0, payoutTotal: 0 };

  // Winners: payout = winning shares x $1, shares = net_stake / price. Exclude
  // anyone who has a SELL on this game (they exited; computing a redeem from
  // BUYs alone would overpay) — left for manual review instead.
  const winners = await pool.query(
    `
    SELECT b.user_address,
           SUM(b.net_stake_dec / (b.avg_price_bps / 10000.0)) AS shares
    FROM public.user_trade_events b
    WHERE b.game_id = $1
      AND b.type = 'BUY'
      AND b.outcome_index = $2
      AND b.avg_price_bps > 0
      AND b.net_stake_dec > 0
      AND NOT EXISTS (
        SELECT 1 FROM public.user_trade_events s
        WHERE s.game_id = b.game_id
          AND lower(s.user_address) = lower(b.user_address)
          AND s.type = 'SELL'
      )
    GROUP BY b.user_address
    `,
    [g.game_id, winningIndex],
  );

  const tsNow = Math.floor(Date.now() / 1000);
  let claimed = 0;
  let payoutTotal = 0;
  for (const w of winners.rows) {
    const payout = Number(w.shares);
    if (!Number.isFinite(payout) || payout <= 0) continue;
    const payoutStr = payout.toFixed(6);
    const user = String(w.user_address).toLowerCase();
    const id = `claim-direct-pm-settle-${g.game_id}-${user}`;
    const tx = `pm-settle-${g.game_id}-${user}`;
    const ins = await pool.query(
      `
      INSERT INTO public.user_trade_events (
        id, user_address, beneficiary_address, game_id, league,
        type, side, outcome_index, outcome_code, timestamp, tx_hash,
        spot_price_bps, avg_price_bps, gross_in_dec, gross_out_dec, fee_dec,
        net_stake_dec, net_out_dec, cost_basis_closed_dec, realized_pnl_dec,
        inserted_at, updated_at
      ) VALUES (
        $1, $2, NULL, $3, $4,
        'CLAIM', NULL, $5, $6, $7, $8,
        NULL, NULL, '0', $9, '0',
        '0', $9, '0', '0',
        now(), now()
      )
      ON CONFLICT (id) DO NOTHING
      `,
      [id, user, g.game_id, g.league, winningIndex, winnerCode, tsNow, tx, payoutStr],
    );
    if (ins.rowCount && ins.rowCount > 0) {
      claimed += 1;
      payoutTotal += payout;
    }
  }
  return { claimed, payoutTotal };
}

/** Map a semantic winner to OUR stored outcome index + code + side, using the
 *  DB game's own team codes (robust to slug/code spelling differences). */
function mapWinner(
  g: OpenGame,
  isSoccer: boolean,
  winner: Winner,
): { index: 0 | 1 | 2; code: string; side: "A" | "B" | null } | null {
  const a = String(g.team_a_code || "").toUpperCase();
  const b = String(g.team_b_code || "").toUpperCase();
  if (winner === "HOME") return a ? { index: 0, code: a, side: "A" } : null;
  if (winner === "DRAW") return isSoccer ? { index: 1, code: "DRAW", side: null } : null;
  // AWAY
  if (!b) return null;
  return { index: isSoccer ? 2 : 1, code: b, side: "B" };
}

let running = false;

export async function runPmSettlementOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const open = await loadOpenGames();
    if (!open.length) return;

    // Build a resolution index per league from Gamma (once).
    const leagues = Array.from(new Set(open.map((g) => g.league.toUpperCase())));
    const byLeague = new Map<string, Resolution[]>();
    for (const lg of leagues) {
      const tag = TAGS[lg];
      if (!tag) continue;
      const isSoccer = lg in SOCCER_TAGS;
      const evs = await fetchResolvedEvents(tag);
      const read = isSoccer ? readSoccer : readBinary;
      byLeague.set(lg, evs.map(read).filter((r): r is Resolution => !!r));
    }

    let settled = 0;
    let claims = 0;
    for (const g of open) {
      const lg = g.league.toUpperCase();
      const isSoccer = lg in SOCCER_TAGS;
      const candidates = byLeague.get(lg) || [];

      const match = candidates.find((r) => {
        if (Math.abs(r.lockTime - Number(g.lock_time)) > 120) return false;
        if (!isSoccer) {
          // Binary: slug codes match ours exactly — strong, handles doubleheaders.
          return pairKey(r.codes[0], r.codes[1]) === pairKey(g.team_a_code, g.team_b_code);
        }
        // Soccer: slug codes may differ from ours, so match on lock-time and,
        // when we have names, a loose home/away name check.
        const an = normName(g.team_a_name || "");
        const bn = normName(g.team_b_name || "");
        if (!an && !bn) return true; // only candidate at this exact lock-time
        const rn = normName(r.homeName) + "|" + normName(r.awayName);
        return (an && rn.includes(an)) || (bn && rn.includes(bn));
      });
      if (!match) continue;

      const mapped = mapWinner(g, isSoccer, match.winner);
      if (!mapped) {
        console.warn(
          `[pm-settle] ${g.game_id} winner ${match.winner} did not map to stored codes ${g.team_a_code}/${g.team_b_code} — skipping`,
        );
        continue;
      }

      try {
        const r = await settleGame(g, mapped.index, mapped.code, mapped.side);
        console.log(
          `[pm-settle] ${g.game_id} -> ${match.winner} (${mapped.code}, idx ${mapped.index}); ${r.claimed} claim(s), $${r.payoutTotal.toFixed(2)} paid`,
        );
        settled += 1;
        claims += r.claimed;
      } catch (e: any) {
        console.error(`[pm-settle] failed to settle ${g.game_id}: ${e?.message || e}`);
      }
    }

    if (settled) console.log(`[pm-settle] run complete: ${settled} game(s), ${claims} claim(s)`);
  } catch (e: any) {
    console.error(`[pm-settle] run error: ${e?.message || e}`);
  } finally {
    running = false;
  }
}

export function startPmSettlementCron(): void {
  if (String(process.env.PM_SETTLE_ENABLED || "true").toLowerCase() === "false") {
    console.log("[pm-settle] disabled (PM_SETTLE_ENABLED=false)");
    return;
  }
  console.log(`[pm-settle] cron enabled — every ${Math.round(INTERVAL_MS / 60000)}m`);
  setTimeout(() => void runPmSettlementOnce(), 30_000);
  setInterval(() => void runPmSettlementOnce(), INTERVAL_MS);
}
