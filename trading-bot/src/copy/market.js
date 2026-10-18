// trading-bot/src/copy/market.js
//
// What the copy fan-out needs to know about a Polymarket market and a
// subscriber's wallet, beyond what the bot's intent carries:
//
//   marketInfo   Gamma: start time, the outcome token order, display names,
//                and the app's game id for this market (so copied trades land in
//                the same History / settlement rows as trades made in the app)
//   tickSize     the CLOB's price grid for a token (orders off it are rejected)
//   heldShares   what the wallet actually holds of a token, read on-chain
//   spendableUsd the wallet's pUSD -- the collateral an order spends
//   userWallets  a subscriber's app identity + Polymarket trading wallet

const { q } = require("../db");
const log = require("../log");

const GAMMA = "https://gamma-api.polymarket.com";
const CLOB = "https://clob.polymarket.com";
const DATA_API = "https://data-api.polymarket.com";
const PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
// Conditional Tokens (ERC-1155): outcome shares live here, 6 decimals.
const CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const POLYGON_RPCS = [
  process.env.POLYGON_RPC_URL || "",
  "https://polygon-bor-rpc.publicnode.com",
  "https://polygon.drpc.org",
].filter(Boolean);

async function getJson(url) {
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`${r.status} ${url}`);
  return r.json();
}
const parseArr = (x) => { try { return Array.isArray(x) ? x : JSON.parse(x || "[]"); } catch { return []; } };

const infoCache = new Map(); // conditionId -> { at, info }
const INFO_TTL_MS = 10 * 60 * 1000;

/**
 * Market facts for a condition id, or null when Gamma does not know it.
 *
 * gameId follows the app's own convention so a copied trade joins the same
 * public.games row as a trade placed in the app: a two-team moneyline is
 * "<league>-<a>-<b>-pm-<start>". Any other market on the game (totals) gets
 * "<that>::ou-<line>" -- its own row, never mistaken for the moneyline by the
 * settlement worker, which settles '::' games by their own market result.
 */
async function marketInfo(conditionId, line) {
  const key = String(conditionId).toLowerCase();
  const hit = infoCache.get(key);
  if (hit && Date.now() - hit.at < INFO_TTL_MS) return hit.info;

  let m = null;
  try {
    const open = await getJson(`${GAMMA}/markets?condition_ids=${key}`);
    m = (Array.isArray(open) && open[0]) || null;
    if (!m) {
      const closed = await getJson(`${GAMMA}/markets?condition_ids=${key}&closed=true`);
      m = (Array.isArray(closed) && closed[0]) || null;
    }
  } catch (e) {
    log.warn(`copy: gamma ${key.slice(0, 10)}…: ${e.message}`);
  }
  if (!m) return null;

  const events = parseArr(m.events);
  const eventSlug = String(events[0]?.slug || m.slug || "");
  const sm = eventSlug.match(/^([a-z]+)-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d{2}-\d{2}/i);
  const start = Math.floor(Date.parse(String(m.gameStartTime || "").replace(" ", "T").replace(/\+00$/, "Z")) / 1000);
  const outcomes = parseArr(m.outcomes).map(String);
  const tokens = parseArr(m.clobTokenIds).map(String);
  const isTotal = /total|o\/u|over/i.test(`${m.sportsMarketType || ""} ${m.question || ""}`) &&
    outcomes.map((o) => o.toLowerCase()).join("|") === "over|under";

  let gameId = null;
  if (sm && start > 0) {
    const base = `${sm[1]}-${sm[2]}-${sm[3]}-pm-${start}`.toLowerCase();
    gameId = isTotal ? `${base}::ou-${line ?? "x"}` : base;
  }
  const lineTxt = line != null && Number.isFinite(Number(line)) ? ` ${Number(line)}` : "";
  const info = {
    conditionId: key,
    gameId,
    gameStartSec: start > 0 ? start : null,
    tokens,
    outcomes,
    question: m.question || null,
    isTotal,
    closed: !!m.closed,
    outcomePrices: parseArr(m.outcomePrices).map(Number),
    meta: sm ? {
      league: sm[1].toUpperCase(),
      lockTime: start > 0 ? start : null,
      teamACode: isTotal ? "OVER" : sm[2].toUpperCase(),
      teamBCode: isTotal ? "UNDER" : sm[3].toUpperCase(),
      teamAName: isTotal ? `Over${lineTxt}` : (outcomes[0] ?? null),
      teamBName: isTotal ? `Under${lineTxt}` : (outcomes[1] ?? null),
      marketType: isTotal ? "TOTAL" : "GAME",
      marketQuestion: m.question || null,
    } : null,
  };
  infoCache.set(key, { at: Date.now(), info });
  return info;
}

/** The outcome index (0/1) of a token within its market, or null. */
function outcomeIndexOf(info, tokenId) {
  const i = info ? info.tokens.indexOf(String(tokenId)) : -1;
  return i === 0 || i === 1 ? i : null;
}

/** Human label for a position: "Broncos", "Over 44.5". */
function positionLabel(info, tokenId) {
  const i = outcomeIndexOf(info, tokenId);
  if (i == null || !info.meta) return null;
  return i === 0 ? info.meta.teamAName : info.meta.teamBName;
}

const tickCache = new Map();
/** The CLOB price grid for a token; 0.01 (valid on either grid) on failure. */
async function tickSize(tokenId) {
  const key = String(tokenId);
  if (tickCache.has(key)) return tickCache.get(key);
  let tick = 0.01;
  try {
    const j = await getJson(`${CLOB}/tick-size?token_id=${key}`);
    const t = Number(j?.minimum_tick_size);
    if (Number.isFinite(t) && t > 0) tick = t;
  } catch (e) {
    log.warn(`copy: tick lookup failed for ${key.slice(0, 10)}…: ${e.message}`);
  }
  tickCache.set(key, tick);
  return tick;
}

/** The subscriber's app identity (users.primary_address) and Polymarket wallet. */
async function userWallets(privyDid) {
  const { rows } = await q(
    `select lower(primary_address) as owner, lower(trading_address) as trading
       from public.users where id = $1`, [privyDid]);
  return { owner: rows[0]?.owner || null, trading: rows[0]?.trading || null };
}

async function ethCall(to, data) {
  for (const rpc of POLYGON_RPCS) {
    try {
      const r = await fetch(rpc, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_call", params: [{ to, data }, "latest"] }),
      });
      const j = await r.json();
      if (j?.result) return BigInt(j.result);
    } catch { /* next rpc */ }
  }
  return null;
}

/**
 * Shares of a token the trading wallet holds now; null when unreadable.
 *
 * Read from the CTF contract, not the data-api: the data-api positions feed
 * lags a fill, and an exit placed seconds after its entry read 0 there and was
 * skipped -- leaving the position with no sell at all. The data-api is only
 * the fallback when every RPC is down.
 */
async function heldShares(tradingWallet, conditionId, tokenId) {
  if (!tradingWallet) return null;
  const data = "0x00fdd58e"
    + tradingWallet.toLowerCase().replace(/^0x/, "").padStart(64, "0")
    + BigInt(tokenId).toString(16).padStart(64, "0");
  const bal = await ethCall(CTF, data);
  if (bal !== null) return Number(bal) / 1e6;
  try {
    const list = await getJson(
      `${DATA_API}/positions?user=${tradingWallet}&market=${conditionId}&sizeThreshold=0`);
    const p = (Array.isArray(list) ? list : []).find((x) => String(x.asset) === String(tokenId));
    return p ? Number(p.size) || 0 : 0;
  } catch (e) {
    log.warn(`copy: position read failed: ${e.message}`);
    return null;
  }
}

/** pUSD the trading wallet can spend (6 decimals), or null when unreadable. */
async function spendableUsd(tradingWallet) {
  if (!tradingWallet) return null;
  const data = "0x70a08231" + tradingWallet.toLowerCase().replace(/^0x/, "").padStart(64, "0");
  for (const rpc of POLYGON_RPCS) {
    try {
      const r = await fetch(rpc, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_call", params: [{ to: PUSD, data }, "latest"] }),
      });
      const j = await r.json();
      if (j?.result) return Number(BigInt(j.result)) / 1e6;
    } catch { /* next rpc */ }
  }
  return null;
}

module.exports = { marketInfo, outcomeIndexOf, positionLabel, tickSize, userWallets, heldShares, spendableUsd };
