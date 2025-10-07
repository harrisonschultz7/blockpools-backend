import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { ethers } from 'ethers';

// ===== Env =====
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const TSDB_KEY = process.env.THESPORTSDB_API_KEY || '1';
const DRY_RUN = process.env.DRY_RUN === '1';
const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

// ===== Paths =====
const GAMES_PATH = path.resolve(__dirname, '..', 'src', 'data', 'games.json');

// ===== ABI =====
import poolAbiJson from '../build/artifacts/contracts/GamePool.sol/GamePool.json';
const poolAbi = (poolAbiJson as any).abi;

// ===== Utils (match your send-request helpers) =====
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
  return `${parts.year}-${parts.month}-${parts.day}`; // YYYY-MM-DD
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
  const haveScores = Number.isFinite(hs) && Number.isFinite(as);
  return isFinished && haveScores;
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

async function fetchDay(leagueKey: string, dayIso: string) {
  if (!dayIso) return [];
  // Map your lowercase league to TheSportsDB league name used in eventsday.php
  const TSDB: Record<string, string> = {
    mlb: 'MLB',
    nfl: 'NFL',
    nba: 'NBA',
    nhl: 'NHL',
    epl: 'English%20Premier%20League',
    ucl: 'UEFA%20Champions%20League',
  };
  const lk = leagueKey.toLowerCase();
  if (!TSDB[lk]) return [];
  const url = `https://www.thesportsdb.com/api/v1/json/${TSDB_KEY}/eventsday.php?d=${dayIso}&l=${TSDB[lk]}`;
  const { data } = await axios.get(url, { timeout: 10000 });
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

// Flatten your grouped games.json into a list of contract addresses.
// We don't need team codes from the file because we read names on-chain.
function loadContractsFromGames(): string[] {
  const raw = fs.readFileSync(GAMES_PATH, 'utf8');
  const grouped = JSON.parse(raw) as Record<string, Array<{ contractAddress: string }>>;
  const addrs: string[] = [];
  for (const key of Object.keys(grouped)) {
    for (const g of grouped[key]) {
      if (g?.contractAddress) addrs.push(g.contractAddress);
    }
  }
  // dedupe
  return Array.from(new Set(addrs));
}

async function main() {
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const contracts = loadContractsFromGames();
  let submitted = 0;

  for (const addr of contracts) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(addr, poolAbi, wallet);

    // Read on-chain config/state (single source of truth)
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
        pool.lockTime ? pool.lockTime().then((x: any) => Number(x)) : 0,
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

    // On-chain gates
    if (!isLocked) continue;
    if (winningTeam !== 0) continue;
    if (requestSent) continue;
    if (lockTime > 0 && Date.now() / 1000 < lockTime + REQUEST_GAP_SECONDS) continue;

    // Off-chain final check (mirror your SOURCE)
    const d0 = epochToEtISO(lockTime);
    const d1 = addDaysISO(d0, 1);

    let picked: any | null = null;
    try {
      const ev0 = await fetchDay(league, d0);
      const ev1 = await fetchDay(league, d1);
      const all = [...ev0, ...ev1];
      picked = pickBestEvent(all, lockTime, teamAName, teamBName);
      if (!picked) {
        // No match found off-chain; skip and let next run try again
        continue;
      }
      if (!statusIsFinal(picked)) {
        // Not final yet
        continue;
      }
    } catch (e) {
      console.error(`[ERR] TSDB query ${addr}:`, (e as Error).message);
      continue;
    }

    // Trigger on-chain request (Functions will double-check and write)
    try {
      if (DRY_RUN) {
        console.log(`[DRY_RUN] Would requestSettlement() on ${addr}  (${league.toUpperCase()} ${teamAName} vs ${teamBName})`);
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
