// @ts-nocheck

// Optional .env for local runs; CI uses secrets. If dotenv isn't present, this no-ops.
try { require('dotenv').config(); } catch {}

import fs from 'fs';
import path from 'path';
import { ethers } from 'ethers';

/** ===== Env ===== */
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const TSDB_KEY = process.env.THESPORTSDB_API_KEY || '1';
const DRY_RUN = process.env.DRY_RUN === '1';
const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

// Explicit override path (for reading games.json from another repo)
const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";

/** ===== Local fallback candidates (if no override) ===== */
const GAMES_CANDIDATES = [
  path.resolve(__dirname, '..', 'src', 'data', 'games.json'),
  path.resolve(__dirname, '..', 'games.json'),
];

/** ===== Minimal ABI (reads + requestSettlement only) ===== */
const poolAbi = [
  { inputs: [], name: 'league', outputs: [{ internalType: 'string', type: 'string' }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'teamAName', outputs: [{ internalType: 'string', type: 'string' }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'teamBName', outputs: [{ internalType: 'string', type: 'string' }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'isLocked', outputs: [{ internalType: 'bool',   type: 'bool'   }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'requestSent', outputs: [{ internalType: 'bool', type: 'bool'   }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'winningTeam', outputs: [{ internalType: 'uint8',type: 'uint8'  }], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'lockTime', outputs: [{ internalType: 'uint256', type: 'uint256'}], stateMutability: 'view', type: 'function' },
  { inputs: [], name: 'requestSettlement', outputs: [], stateMutability: 'nonpayable', type: 'function' },
] as const;

/** ===== Utils (mirror your send-request helpers) ===== */
function epochToEtISO(epochSec: number) {
  const dt = new Date(epochSec * 1000);
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts: Record<string, string> = {};
  for (const p of fmt.formatToParts(dt)) parts[p.type] = p.value;
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function addDaysISO(iso: string, days: number) {
  const [y, m, d] = iso.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  const y2 = dt.getUTCFullYear();
  const m2 = String(dt.getUTCMonth() + 1).padStart(2, '0');
  const d2 = String(dt.getUTCDate()).padStart(2, '0');
  return `${y2}-${m2}-${d2}`;
}

const f = (s: string) => (s || '').trim().toLowerCase();

function statusIsFinal(evt: any) {
  const statusU = String(evt?.strStatus || '').toUpperCase();
  const prog = String(evt?.strProgress || '');
  const isFinished =
    /^(FT|AOT|AET|PEN|FINISHED)$/.test(statusU) ||
    /final/i.test(statusU) ||
    /final/i.test(prog);
  const hs = Number(evt?.intHomeScore ?? NaN);
  const as = Number(evt?.intAwayScore ?? NaN);
  return isFinished && Number.isFinite(hs) && Number.isFinite(as);
}

function toEpoch(evt: any) {
  const ts = evt?.strTimestamp || '';
  if (ts) {
    const ms = Date.parse(ts);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  const de = evt?.dateEvent;
  const tm = evt?.strTime;
  if (de && tm) {
    let iso = `${de}T${tm}`;
    if (!/Z$/.test(iso)) iso += 'Z';
    const ms = Date.parse(iso);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  if (de) {
    const ms = Date.parse(`${de}T00:00:00Z`);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  return null;
}

// Node 20: global fetch + AbortSignal.timeout
async function fetchJSON(url: string, timeoutMs = 10000) {
  const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function fetchDay(leagueKey: string, dayIso: string) {
  if (!dayIso) return [];
  const TSDB: Record<string, string> = {
    mlb: 'MLB',
    nfl: 'NFL',
    nba: 'NBA',
    nhl: 'NHL',
    epl: 'English%20Premier%20League',
    ucl: 'UEFA%20Champions%20League',
  };
  const lk = (leagueKey || '').toLowerCase();
  if (!TSDB[lk]) return [];
  const url = `https://www.thesportsdb.com/api/v1/json/${TSDB_KEY}/eventsday.php?d=${dayIso}&l=${TSDB[lk]}`;
  const data = await fetchJSON(url, 10000);
  const ev = (data && data.events) || [];
  return Array.isArray(ev) ? ev : [];
}

function pickBestEvent(events: any[], startEpoch: number, nameA: string, nameB: string) {
  const A = f(nameA), B = f(nameB);
  const candidates: { e: any; ep: number | null }[] = [];
  for (const e of events) {
    const home = f(e.strHomeTeam), away = f(e.strAwayTeam);
    const isMatch = (home === A && away === B) || (home === B && away === A);
    if (isMatch) candidates.push({ e, ep: toEpoch(e) });
  }
  if (!candidates.length) return null;
  candidates.sort((a, b) => {
    const da = a.ep == null ? 1e15 : Math.abs(a.ep - startEpoch);
    const db = b.ep == null ? 1e15 : Math.abs(b.ep - startEpoch);
    return da - db || ((a.ep || 0) - (b.ep || 0));
  });
  return candidates[0].e;
}

/** ===== Input discovery ===== */
function readGamesAtPath(p: string): string[] | null {
  if (!fs.existsSync(p)) return null;
  try {
    const raw = fs.readFileSync(p, 'utf8');
    const grouped = JSON.parse(raw) as Record<string, Array<{ contractAddress: string }>>;
    const addrs = Object.values(grouped).flat().map((g) => g?.contractAddress).filter(Boolean);
    const uniq = Array.from(new Set(addrs));
    if (uniq.length) {
      console.log(`Using games from ${p} (${uniq.length} contracts)`);
      return uniq;
    }
  } catch (e) {
    console.warn(`Failed to parse ${p}:`, (e as Error).message);
  }
  return null;
}

function loadContractsFromGames(): string[] {
  // 0) Explicit override: use games.json from another repo/location
  if (GAMES_PATH_OVERRIDE) {
    const fromOverride = readGamesAtPath(GAMES_PATH_OVERRIDE);
    if (fromOverride) return fromOverride;
    console.warn(`GAMES_PATH was set but not readable/usable: ${GAMES_PATH_OVERRIDE}`);
  }

  // 1) Local candidates (repo files)
  for (const p of GAMES_CANDIDATES) {
    const fromLocal = readGamesAtPath(p);
    if (fromLocal) return fromLocal;
  }

  // 2) Env fallback (comma/space-separated addresses)
  const envList = (process.env.CONTRACTS || '').trim();
  if (envList) {
    const arr = envList.split(/[,\s]+/).filter(Boolean);
    const filtered = arr.filter((a) => {
      try { return ethers.isAddress(a); } catch { return false; }
    });
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered));
    }
  }

  console.warn('No contracts found in games.json or CONTRACTS env. Nothing to do.');
  return [];
}

/** ===== Main ===== */
async function main() {
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const contracts = loadContractsFromGames();
  if (!contracts.length) return;

  let submitted = 0;

  for (const addr of contracts) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(addr, poolAbi, wallet);

    let league: string, teamAName: string, teamBName: string;
    let isLocked: boolean, requestSent: boolean, winningTeam: number, lockTime: number;
    try {
      const [lg, ta, tb, locked, req, win, lt] = await Promise.all([
        pool.league(),
        pool.teamAName(),
        pool.teamBName(),
        pool.isLocked(),
        pool.requestSent(),
        pool.winningTeam().then((x: any) => Number(x)),
        pool.lockTime().then((x: any) => Number(x)),
      ]);
      league = String(lg || '').toLowerCase();
      teamAName = String(ta || '');
      teamBName = String(tb || '');
      isLocked = Boolean(locked);
      requestSent = Boolean(req);
      winningTeam = Number(win);
      lockTime = Number(lt);
    } catch (e) {
      console.error(`[ERR] read state ${addr}:`, (e as Error).message);
      continue;
    }

    if (!isLocked) continue;
    if (winningTeam !== 0) continue;
    if (requestSent) continue;
    if (lockTime > 0 && Date.now() / 1000 < lockTime + REQUEST_GAP_SECONDS) continue;

    const d0 = epochToEtISO(lockTime);
    const d1 = addDaysISO(d0, 1);

    let picked: any | null = null;
    try {
      const ev0 = await fetchDay(league, d0);
      const ev1 = await fetchDay(league, d1);
      picked = pickBestEvent([...ev0, ...ev1], lockTime, teamAName, teamBName);
      if (!picked) continue;
      if (!statusIsFinal(picked)) continue;
    } catch (e) {
      console.error(`[ERR] TSDB query ${addr}:`, (e as Error).message);
      continue;
    }

    try {
      if (DRY_RUN) {
        console.log(`[DRY_RUN] Would call requestSettlement() on ${addr}  (${league.toUpperCase()} ${teamAName} vs ${teamBName})`);
      } else {
        const tx = await pool.requestSettlement();
        console.log(`[OK] requestSettlement sent for ${addr}: ${tx.hash}`);
      }
      submitted++;
    } catch (e) {
      console.error(`[ERR] requestSettlement ${addr}:`, (e as Error).message);
    }
  }

  console.log(`Submitted ${submitted} request(s) this run`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
