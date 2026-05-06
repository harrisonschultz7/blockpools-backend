// src/scripts/refreshGamesAndScores.ts
//
// Recurring sync job — keeps two tables fresh:
//
//   1. public.games
//      Reads pool addresses from src/data/games.json. SELECTs which ones are
//      already in the table. For ONLY the missing addresses, does on-chain
//      reads (lockTime, league, team codes/names, winningTeam, market type
//      detection via outcomesCount probe) and INSERTs a row. Steady-state
//      RPC cost is zero — reads only fire when a brand-new pool appears in
//      games.json.
//
//   2. public.game_score_cache
//      For every game in games whose lock_time falls inside the active
//      window (now - 24h .. now + 48h) and is not yet final, calls the
//      existing /api/scores/live endpoint over loopback with ?force=1 to
//      bypass the 15-min cache. The endpoint writes a fresh row into
//      game_score_cache; the live-score ticker reads from that table.
//
// Designed to be invoked by a systemd timer every 5 minutes. See the
// blockpools-refresh-games.{service,timer} units for the install pattern.

import "dotenv/config";

import * as fs from "fs";
import * as path from "path";
import { JsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";

import { pool } from "../db";
// Default fallback — the bundled games.json the backend ships with.
import bundledGamesJson from "../data/games.json";

// Path override: when set, read games.json from this file at runtime instead
// of using the bundled copy. Lets the VPS point at the frontend repo's
// games.json so there's a single source of truth.
const GAMES_JSON_PATH = (process.env.GAMES_JSON_PATH || "").trim();

const RPC_URL =
  process.env.RPC_URL ||
  process.env.PROMO_RPC_URL ||
  process.env.ARBITRUM_RPC_URL ||
  "https://arb1.arbitrum.io/rpc";

const PORT = Number(process.env.PORT || 3001);

const SCORE_REFRESH_HOURS_AFTER = Number(
  process.env.REFRESH_GAMES_HOURS_AFTER_LOCK || 24
);
const SCORE_REFRESH_HOURS_BEFORE = Number(
  process.env.REFRESH_GAMES_HOURS_BEFORE_LOCK || 48
);
const RPC_CONCURRENCY = Number(process.env.REFRESH_GAMES_RPC_CONCURRENCY || 5);
const SCORE_HTTP_CONCURRENCY = Number(
  process.env.REFRESH_GAMES_SCORE_CONCURRENCY || 4
);

// Read-only ABI snapshot covering both binary (gamePool) and multi
// (gamePoolMulti) variants. Each call is wrapped in a try/catch so a
// missing method on a given pool just yields null.
const POOL_ABI = [
  "function lockTime() view returns (uint256)",
  "function isLocked() view returns (bool)",
  "function league() view returns (string)",
  "function teamACode() view returns (string)",
  "function teamBCode() view returns (string)",
  "function teamAName() view returns (string)",
  "function teamBName() view returns (string)",
  // binary
  "function winningTeam() view returns (uint8)",
  // multi
  "function outcomesCount() view returns (uint8)",
  "function isResolved() view returns (bool)",
  "function winningOutcomeIndex() view returns (uint8)",
];

type JsonGame = {
  contractAddress: string;
  league?: string;
  teamA?: string;
  teamB?: string;
  teamACode?: string;
  teamBCode?: string;
  teamAName?: string;
  teamBName?: string;
  marketType?: string;
  topic?: string;
  marketQuestion?: string;
  marketShort?: string;
  date?: string;
  time?: string;
  lockTime?: number | string;
  Location?: string;
  location?: string;
};

function loadGamesJson(): JsonGame[] {
  let raw: Record<string, JsonGame[]>;
  let source = "bundled";

  // Prefer the runtime path override so the cron stays in sync with whatever
  // the frontend ships, without duplicating the file in two repos.
  if (GAMES_JSON_PATH) {
    try {
      const abs = path.isAbsolute(GAMES_JSON_PATH)
        ? GAMES_JSON_PATH
        : path.resolve(process.cwd(), GAMES_JSON_PATH);
      const txt = fs.readFileSync(abs, "utf8");
      raw = JSON.parse(txt) as Record<string, JsonGame[]>;
      source = abs;
    } catch (err: any) {
      console.warn(
        `[refreshGamesAndScores] GAMES_JSON_PATH=${GAMES_JSON_PATH} unreadable, ` +
          `falling back to bundled games.json: ${err?.message ?? err}`
      );
      raw = bundledGamesJson as unknown as Record<string, JsonGame[]>;
    }
  } else {
    raw = bundledGamesJson as unknown as Record<string, JsonGame[]>;
  }

  const out: JsonGame[] = [];
  for (const [topLeague, arr] of Object.entries(raw)) {
    if (!Array.isArray(arr)) continue;
    for (const g of arr) {
      if (!g || typeof g !== "object") continue;
      if (!g.contractAddress) continue;
      out.push({ ...g, league: g.league || topLeague });
    }
  }
  console.log(`[refreshGamesAndScores] loaded ${out.length} pool(s) from ${source}`);
  return out;
}

type ChainMeta = {
  lockTime: number | null;
  isLocked: boolean | null;
  league: string | null;
  teamACode: string | null;
  teamBCode: string | null;
  teamAName: string | null;
  teamBName: string | null;
  winningTeam: number | null;
  outcomesCount: number | null;
  isResolved: boolean | null;
  winningOutcomeIndex: number | null;
};

async function readChainMetadata(
  addr: string,
  provider: JsonRpcProvider
): Promise<ChainMeta> {
  const c = new Contract(addr, POOL_ABI, provider);
  const safe = async <T>(fn: () => Promise<T>): Promise<T | null> => {
    try {
      return await fn();
    } catch {
      return null;
    }
  };
  const lockTime = await safe<number>(() => c.lockTime().then((x: any) => Number(x.toString())));
  const isLocked = await safe<boolean>(() => c.isLocked());
  const league = await safe<string>(() => c.league());
  const teamACode = await safe<string>(() => c.teamACode());
  const teamBCode = await safe<string>(() => c.teamBCode());
  const teamAName = await safe<string>(() => c.teamAName());
  const teamBName = await safe<string>(() => c.teamBName());
  const winningTeam = await safe<number>(() => c.winningTeam().then((x: any) => Number(x.toString())));
  const outcomesCount = await safe<number>(() => c.outcomesCount().then((x: any) => Number(x.toString())));
  const isResolved = await safe<boolean>(() => c.isResolved());
  const winningOutcomeIndex = await safe<number>(() =>
    c.winningOutcomeIndex().then((x: any) => Number(x.toString()))
  );

  return {
    lockTime,
    isLocked,
    league,
    teamACode,
    teamBCode,
    teamAName,
    teamBName,
    winningTeam,
    outcomesCount,
    isResolved,
    winningOutcomeIndex,
  };
}

async function findMissingAddresses(
  candidateAddresses: string[]
): Promise<string[]> {
  if (!candidateAddresses.length) return [];
  const r = await pool.query<{ game_id: string }>(
    `SELECT lower(game_id) AS game_id
       FROM public.games
      WHERE lower(game_id) = ANY($1::text[])`,
    [candidateAddresses]
  );
  const present = new Set(r.rows.map((row) => row.game_id));
  return candidateAddresses.filter((a) => !present.has(a));
}

function pickStr(...vals: Array<string | null | undefined>): string | null {
  for (const v of vals) {
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return null;
}

function pickUpperStr(...vals: Array<string | null | undefined>): string | null {
  const s = pickStr(...vals);
  return s ? s.toUpperCase() : null;
}

function deriveBinaryFinalState(meta: ChainMeta): {
  isFinal: boolean;
  winningOutcomeIndex: number | null;
} {
  // Binary contract: winningTeam() returns 0=unresolved, 1=A, 2=B, 3=tie/draw.
  // Map onto outcome_index where 0=A, 1=B, 2=draw — matches user_trade_events
  // outcome_index conventions used elsewhere in this codebase.
  const wt = meta.winningTeam ?? 0;
  if (!wt) return { isFinal: false, winningOutcomeIndex: null };
  if (wt === 1) return { isFinal: true, winningOutcomeIndex: 0 };
  if (wt === 2) return { isFinal: true, winningOutcomeIndex: 1 };
  if (wt === 3) return { isFinal: true, winningOutcomeIndex: 2 };
  return { isFinal: false, winningOutcomeIndex: null };
}

async function insertGameRow(jg: JsonGame, meta: ChainMeta): Promise<boolean> {
  const game_id = String(jg.contractAddress).toLowerCase();
  const league = pickUpperStr(meta.league, jg.league);

  const lockTimeNum =
    meta.lockTime ??
    (jg.lockTime != null && Number.isFinite(Number(jg.lockTime))
      ? Number(jg.lockTime)
      : null);

  const team_a_code = pickUpperStr(jg.teamACode, jg.teamA, meta.teamACode);
  const team_b_code = pickUpperStr(jg.teamBCode, jg.teamB, meta.teamBCode);
  const team_a_name = pickStr(jg.teamAName, meta.teamAName);
  const team_b_name = pickStr(jg.teamBName, meta.teamBName);

  // Detect market type:
  //   - games.json may state it explicitly (PROP / MULTI)
  //   - else if outcomesCount probed successfully → MULTI (regardless of count)
  //   - else (probe reverted) → BINARY
  const market_type = pickUpperStr(jg.marketType) ??
    (meta.outcomesCount != null ? "MULTI" : "BINARY");

  const outcomes_count =
    meta.outcomesCount ??
    (market_type === "BINARY" ? 2 : null);

  // Final + winning outcome:
  //   - Multi pool: trust isResolved + winningOutcomeIndex
  //   - Binary pool: derive from winningTeam
  let is_final = false;
  let winning_outcome_index: number | null = null;
  if (meta.isResolved === true) {
    is_final = true;
    winning_outcome_index =
      meta.winningOutcomeIndex != null ? meta.winningOutcomeIndex : null;
  } else {
    const bin = deriveBinaryFinalState(meta);
    is_final = bin.isFinal;
    winning_outcome_index = bin.winningOutcomeIndex;
  }

  // winner_side / winner_team_code only apply to binary outcomes; leave null
  // for multi or unresolved.
  let winner_side: "A" | "B" | null = null;
  let winner_team_code: string | null = null;
  if (is_final && market_type === "BINARY") {
    if (winning_outcome_index === 0) {
      winner_side = "A";
      winner_team_code = team_a_code;
    } else if (winning_outcome_index === 1) {
      winner_side = "B";
      winner_team_code = team_b_code;
    }
  }

  const topic = pickStr(jg.topic);
  const market_question = pickStr(jg.marketQuestion);
  const market_short = pickStr(jg.marketShort);

  // ON CONFLICT DO NOTHING — we never overwrite existing rows. The settlement
  // bot and persistTrades own update paths for is_final / winning fields.
  const r = await pool.query(
    `
    INSERT INTO public.games
      (game_id, league, lock_time, is_final,
       winner_side, winner_team_code,
       market_type, outcomes_count,
       resolution_type, winning_outcome_index,
       team_a_code, team_b_code, team_a_name, team_b_name,
       topic, market_question, market_short)
    VALUES
      ($1, $2, $3, $4,
       $5, $6,
       $7, $8,
       $9, $10,
       $11, $12, $13, $14,
       $15, $16, $17)
    ON CONFLICT (game_id) DO NOTHING
    RETURNING game_id
    `,
    [
      game_id,
      league,
      lockTimeNum,
      is_final,
      winner_side,
      winner_team_code,
      market_type,
      outcomes_count,
      is_final ? "RESOLVED" : "UNRESOLVED",
      winning_outcome_index,
      team_a_code,
      team_b_code,
      team_a_name,
      team_b_name,
      topic,
      market_question,
      market_short,
    ]
  );
  return (r.rowCount ?? 0) > 0;
}

async function syncMissingGames(provider: JsonRpcProvider): Promise<{
  inspected: number;
  inserted: number;
  failed: number;
}> {
  const all = loadGamesJson();
  const allAddresses = all.map((g) => String(g.contractAddress).toLowerCase());
  const missing = await findMissingAddresses(allAddresses);

  if (!missing.length) {
    return { inspected: all.length, inserted: 0, failed: 0 };
  }

  console.log(
    `[refreshGamesAndScores] ${missing.length} pool(s) missing from games — reading on-chain`
  );

  const missingSet = new Set(missing);
  const queue = all.filter((g) =>
    missingSet.has(String(g.contractAddress).toLowerCase())
  );

  let inserted = 0;
  let failed = 0;
  let cursor = 0;

  await Promise.all(
    Array.from({ length: Math.min(RPC_CONCURRENCY, queue.length) }, async () => {
      while (cursor < queue.length) {
        const i = cursor++;
        const jg = queue[i];
        try {
          const meta = await readChainMetadata(jg.contractAddress, provider);
          const did = await insertGameRow(jg, meta);
          if (did) {
            inserted++;
            console.log(
              `[refreshGamesAndScores] inserted ${jg.contractAddress.toLowerCase()} (${jg.league || meta.league || "??"})`
            );
          }
        } catch (err: any) {
          failed++;
          console.warn(
            `[refreshGamesAndScores] failed ${jg.contractAddress}: ${err?.message ?? err}`
          );
        }
      }
    })
  );

  return { inspected: all.length, inserted, failed };
}

async function refreshScoreCache(): Promise<{
  considered: number;
  refreshed: number;
  failed: number;
}> {
  const nowSec = Math.floor(Date.now() / 1000);
  const minLock = nowSec - SCORE_REFRESH_HOURS_AFTER * 3600;
  const maxLock = nowSec + SCORE_REFRESH_HOURS_BEFORE * 3600;

  const r = await pool.query<{
    game_id: string;
    league: string | null;
    team_a_name: string | null;
    team_b_name: string | null;
    team_a_code: string | null;
    team_b_code: string | null;
    lock_time: string | null;
  }>(
    `SELECT game_id, league, team_a_name, team_b_name, team_a_code, team_b_code, lock_time
       FROM public.games
      WHERE COALESCE(is_final, false) = false
        AND lock_time IS NOT NULL
        AND lock_time BETWEEN $1::bigint AND $2::bigint`,
    [minLock, maxLock]
  );

  const games = r.rows;
  if (!games.length) return { considered: 0, refreshed: 0, failed: 0 };

  let refreshed = 0;
  let failed = 0;
  let cursor = 0;

  await Promise.all(
    Array.from(
      { length: Math.min(SCORE_HTTP_CONCURRENCY, games.length) },
      async () => {
        while (cursor < games.length) {
          const i = cursor++;
          const g = games[i];

          // Score endpoint requires teamAName, teamBName, league. If we don't
          // have them in the row, fall back to team codes for the request
          // (better than skipping — Goalserve fuzzy-matches reasonably).
          const teamAName = g.team_a_name || g.team_a_code || "";
          const teamBName = g.team_b_name || g.team_b_code || "";
          const league = g.league || "";

          if (!teamAName || !teamBName || !league) continue;

          const params = new URLSearchParams({
            league,
            teamAName,
            teamBName,
            teamACode: g.team_a_code || "",
            teamBCode: g.team_b_code || "",
            lockTime: String(g.lock_time || ""),
            contractAddress: g.game_id,
            force: "1",
          });

          const url = `http://127.0.0.1:${PORT}/api/scores/live?${params.toString()}`;
          try {
            const res = await fetch(url);
            if (!res.ok) {
              failed++;
              console.warn(
                `[refreshGamesAndScores] score ${g.game_id} HTTP ${res.status}`
              );
            } else {
              refreshed++;
            }
          } catch (err: any) {
            failed++;
            console.warn(
              `[refreshGamesAndScores] score ${g.game_id} threw: ${err?.message ?? err}`
            );
          }
        }
      }
    )
  );

  return { considered: games.length, refreshed, failed };
}

async function main(): Promise<void> {
  const t0 = Date.now();
  const provider = new JsonRpcProvider(RPC_URL);

  let gamesStats = { inspected: 0, inserted: 0, failed: 0 };
  try {
    gamesStats = await syncMissingGames(provider);
  } catch (err) {
    console.error("[refreshGamesAndScores] syncMissingGames failed", err);
    process.exitCode = 1;
  }

  let scoreStats = { considered: 0, refreshed: 0, failed: 0 };
  try {
    scoreStats = await refreshScoreCache();
  } catch (err) {
    console.error("[refreshGamesAndScores] refreshScoreCache failed", err);
    process.exitCode = 1;
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(
    `[refreshGamesAndScores] done in ${elapsed}s — ` +
      `games inspected=${gamesStats.inspected} inserted=${gamesStats.inserted} failed=${gamesStats.failed} | ` +
      `scores considered=${scoreStats.considered} refreshed=${scoreStats.refreshed} failed=${scoreStats.failed}`
  );
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[refreshGamesAndScores] unhandled", err);
    process.exit(1);
  });
