// trading-bot/src/http.js
//
// Fetch + CSV helpers. Node 20 has global fetch, so no dependency is added.
// The CSV parser is quote-aware because nflverse fields contain commas inside
// quotes (player names, "Smith, Jr."), which a naive split would corrupt --
// silently shifting every column after it.

const log = require("./log");

async function getJson(url, { timeoutMs = 30_000, retries = 2, headers = {} } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: ac.signal, headers });
      clearTimeout(t);
      if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
      return await res.json();
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      if (attempt < retries) await sleep(500 * (attempt + 1));
    }
  }
  throw lastErr;
}

async function getText(url, { timeoutMs = 120_000, retries = 2 } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: ac.signal, redirect: "follow" });
      clearTimeout(t);
      if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
      return await res.text();
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      if (attempt < retries) await sleep(1000 * (attempt + 1));
    }
  }
  throw lastErr;
}

/** Quote-aware CSV -> array of objects keyed by header. */
function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += c;
    } else if (c === '"') inQuotes = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; }
    else if (c !== "\r") field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  if (!rows.length) return [];
  const header = rows[0];
  return rows.slice(1)
    .filter((r) => r.length === header.length)
    .map((r) => {
      const o = {};
      for (let i = 0; i < header.length; i++) o[header[i]] = r[i];
      return o;
    });
}

/** nflverse writes "NA" for missing -- distinct from empty string. */
function num(v) {
  if (v === undefined || v === null || v === "" || v === "NA") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function str(v) {
  if (v === undefined || v === null || v === "" || v === "NA") return null;
  return String(v);
}
function bool(v) {
  const n = num(v);
  if (n === null) return null;
  return n !== 0;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

module.exports = { getJson, getText, parseCsv, num, str, bool, sleep, log };

/**
 * Streaming, column-projecting CSV reader.
 *
 * parseCsv() materialises every row as a full object, which for an nflverse
 * play-by-play season (~50k rows x ~400 columns) is ~20M strings and exhausts
 * the default heap. This walks the same state machine but keeps only the
 * columns in `wanted`, handing one slim object at a time to `onRow`, so peak
 * memory is the source text plus a single row.
 */
function streamCsv(text, wanted, onRow) {
  const want = new Set(wanted);
  let header = null;
  let keepIdx = null;          // index -> output key, for wanted columns only
  let row = [];
  let field = "";
  let inQuotes = false;
  let count = 0;

  const flushRow = () => {
    row.push(field);
    field = "";
    if (!header) {
      header = row;
      keepIdx = new Map();
      header.forEach((h, i) => { if (want.has(h)) keepIdx.set(i, h); });
      row = [];
      return;
    }
    if (row.length === header.length) {
      const o = {};
      for (const [i, key] of keepIdx) o[key] = row[i];
      onRow(o);
      count++;
    }
    row = [];
  };

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += c;
    } else if (c === '"') inQuotes = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") flushRow();
    else if (c !== "\r") field += c;
  }
  if (field.length || row.length) flushRow();
  return count;
}

module.exports.streamCsv = streamCsv;
