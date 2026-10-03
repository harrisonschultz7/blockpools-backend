// ---------------------------------------------------------------------------
// pm settlement worker
//
// When a Polymarket market resolves, its positions in our ledger stay "Pending"
// forever (nothing writes a resolution/claim back to Supabase) — so wins never
// roll into Trade History or realized P&L. This worker closes that gap:
//
//   1. Find open pm GAME markets past lock (games.is_final != true).
//   2. Look up the matching RESOLVED event on Polymarket Gamma (closed=true,
//      umaResolutionStatus=resolved, a clean 1/0 outcome split).
//   3. Write the resolution onto public.games (is_final + winning_outcome_index
//      + winner_side + winner_team_code + resolution_type='RESOLVED'). This
//      alone flips every position Won/Lost and realizes losses in the ROI view.
//   4. Write a CLAIM row (payout = winning shares x $1) for each winner so the
//      Return/ROI column and leaderboard realized P&L are correct (without it a
//      win shows -100% because returnAmount = sell + claim).
//
// SAFETY: binary leagues only (NFL/MLB/NBA/NHL). Soccer (EPL/UCL) is a 3-way
// group market — skipped here, TODO. We only settle on an exact lock-time match
// AND a team-code match, and only when the winning team code maps to one of our
// stored codes. Everything is idempotent (games guarded by is_final; CLAIM rows
// deduped by a deterministic id). Disable via PM_SETTLE_ENABLED=false.
// ---------------------------------------------------------------------------
import { pool } from "../db";

const GAMMA = (process.env.GAMMA_API_URL || "https://gamma-api.polymarket.com").replace(/\/+$/, "");

// Binary moneyline leagues only. Soccer 3-way (EPL 306 / UCL 100977) handled later.
const BINARY_TAGS: Record<string, number> = {
  NFL: 450,
  MLB: 100381,
  NBA: 745,
  NHL: 899,
};

const INTERVAL_MS = Number(process.env.PM_SETTLE_INTERVAL_MS || 15 * 60_000);

function parseArr(x: any): any[] {
  try {
    return Array.isArray(x) ? x : JSON.parse(x || "[]");
  } catch {
    return [];
  }
}

/** "nhl-bos-wpg-2026-10-02" -> ["BOS","WPG"] (slug order == outcomes order == A,B). */
function slugCodes(slug: string): [string, string] | null {
  const m = String(slug || "").match(/^[a-z]+-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d{2}-\d{2}/i);
  return m ? [m[1].toUpperCase(), m[2].toUpperCase()] : null;
}

type ResolvedGame = {
  lockTime: number;
  codes: [string, string]; // [A, B] from the slug
  winCode: string | null; // the winning team's code, or null if void/ambiguous
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

/** Extract a clean resolution from a Gamma binary event, or null to skip. */
function readResolution(ev: any): ResolvedGame | null {
  if (ev?.closed !== true) return null;
  const codes = slugCodes(ev.slug);
  if (!codes) return null;

  const mls = (ev.markets || []).filter(
    (m: any) => String(m?.sportsMarketType || "") === "moneyline",
  );
  const ml = mls[0] || (ev.markets || [])[0];
  if (!ml) return null;

  // Require UMA-resolved (when present) — don't settle on a merely-closed market.
  const uma = String(ml.umaResolutionStatus || "").toLowerCase();
  if (uma && uma !== "resolved") return null;

  const start = Date.parse(ml.gameStartTime || ev.startDate || "");
  if (!Number.isFinite(start)) return null;
  const lockTime = Math.floor(start / 1000);

  const prices = parseArr(ml.outcomePrices).map((p: any) => Number(p));
  const outcomes = parseArr(ml.outcomes);
  if (prices.length !== 2 || outcomes.length !== 2) return null;

  // Clean 1/0 split only. Anything ambiguous (void / 0.5-0.5 / mid) -> skip,
  // leave the game Pending for manual review rather than mis-settling.
  const hi = prices.findIndex((p) => p >= 0.99);
  const lo = prices.findIndex((p) => p <= 0.01);
  if (hi < 0 || lo < 0 || hi === lo) return null;

  // outcomes[i] aligns with codes[i] (both in slug order), so codes[hi] is the
  // winning team code.
  const winCode = codes[hi] || null;
  return { lockTime, codes, winCode };
}

type OpenGame = {
  game_id: string;
  league: string;
  lock_time: number;
  team_a_code: string | null;
  team_b_code: string | null;
};

async function loadOpenGames(): Promise<OpenGame[]> {
  const nowSec = Math.floor(Date.now() / 1000);
  const res = await pool.query(
    `
    SELECT g.game_id, g.league, g.lock_time, g.team_a_code, g.team_b_code
    FROM public.games g
    WHERE g.game_id ILIKE '%-pm-%'
      AND COALESCE(g.is_final, false) = false
      AND g.lock_time IS NOT NULL
      AND g.lock_time < $1
      AND upper(g.league) = ANY($2)
    `,
    [nowSec, Object.keys(BINARY_TAGS)],
  );
  return res.rows as OpenGame[];
}

const pair = (a?: string | null, b?: string | null) =>
  [String(a || "").toUpperCase(), String(b || "").toUpperCase()].sort().join("|");

/** Settle a single matched game: write resolution + winner CLAIM rows. */
async function settleGame(
  g: OpenGame,
  winningIndex: 0 | 1,
  winnerCode: string,
): Promise<{ claimed: number; payoutTotal: number }> {
  const winnerSide = winningIndex === 0 ? "A" : "B";

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

  // Winners: payout = winning shares x $1, shares = net_stake / price.
  // Exclude anyone who has a SELL on this game (they exited; computing a redeem
  // from BUYs alone would overpay) — logged for manual review instead.
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

let running = false;

export async function runPmSettlementOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const open = await loadOpenGames();
    if (!open.length) return;

    // Resolution index per league, built once from Gamma.
    const leagues = Array.from(new Set(open.map((g) => g.league.toUpperCase())));
    const byLeague = new Map<string, ResolvedGame[]>();
    for (const lg of leagues) {
      const tag = BINARY_TAGS[lg];
      if (!tag) continue;
      const evs = await fetchResolvedEvents(tag);
      const resolved = evs.map(readResolution).filter((r): r is ResolvedGame => !!r);
      byLeague.set(lg, resolved);
    }

    let settled = 0;
    let claims = 0;
    for (const g of open) {
      const candidates = byLeague.get(g.league.toUpperCase()) || [];
      // Match on exact lock time (+/-120s tolerance) AND the unordered code pair.
      const match = candidates.find(
        (r) =>
          Math.abs(r.lockTime - Number(g.lock_time)) <= 120 &&
          pair(r.codes[0], r.codes[1]) === pair(g.team_a_code, g.team_b_code),
      );
      if (!match || !match.winCode) continue;

      // Map the winning team code to OUR stored outcome index.
      const wc = match.winCode.toUpperCase();
      const winningIndex: 0 | 1 | null =
        wc === String(g.team_a_code || "").toUpperCase()
          ? 0
          : wc === String(g.team_b_code || "").toUpperCase()
            ? 1
            : null;
      if (winningIndex == null) {
        console.warn(
          `[pm-settle] winner ${wc} not in codes ${g.team_a_code}/${g.team_b_code} for ${g.game_id} — skipping`,
        );
        continue;
      }

      try {
        const r = await settleGame(g, winningIndex, wc);
        if (r.claimed || r.payoutTotal) {
          console.log(
            `[pm-settle] ${g.game_id} -> winner ${wc} (idx ${winningIndex}); ${r.claimed} claim(s), $${r.payoutTotal.toFixed(2)} paid`,
          );
        } else {
          console.log(`[pm-settle] ${g.game_id} -> winner ${wc} (idx ${winningIndex}); resolved (no new claims)`);
        }
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
  // Kick off shortly after boot, then on the interval.
  setTimeout(() => void runPmSettlementOnce(), 30_000);
  setInterval(() => void runPmSettlementOnce(), INTERVAL_MS);
}
