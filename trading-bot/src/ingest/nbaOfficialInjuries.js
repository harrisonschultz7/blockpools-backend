// trading-bot/src/ingest/nbaOfficialInjuries.js
//
// The NBA's OFFICIAL injury report -> sports.nba_official_injuries (status
// changes only). Research logging, 2026-11-23: our ESPN injury feed logs a big
// OUT a median ~6 minutes AFTER Polymarket has priced half of it, so before
// paying for faster news we measure whether the league's own report -- the
// source ESPN copies -- arrives early enough to matter.
//
// The league republishes one PDF on 15-minute marks:
//   https://ak-static.cms.nba.com/referee/injury/Injury-Report_YYYY-MM-DD_HH_MMAM.pdf
// (times US-Eastern). Each run looks back two hours of marks, reads any it has
// not read yet (oldest first, so status order is right), and logs every player
// whose status for a game differs from his last logged one, stamped with when
// WE first saw it.
//
// Parsing: the PDF is a table. pdf.js gives each word with its x/y position;
// words are grouped into lines by y and assigned to columns by x. The date,
// time, matchup and team columns are printed only on a group's first line, so
// they carry down.

const pdfjs = require("pdfjs-dist/legacy/build/pdf.js");
const { q } = require("../db");
const log = require("../log");

const BASE = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_";
const LOOKBACK_MARKS = 8; // 2 hours of 15-minute marks

/** Wall-clock time in New York -> UTC Date. */
function etToUtc(y, mo, d, h, mi) {
  const guess = Date.UTC(y, mo - 1, d, h, mi);
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23",
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })
    .formatToParts(new Date(guess)).reduce((o, p) => ((o[p.type] = p.value), o), {});
  const asEt = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour, +parts.minute);
  return new Date(guess + (guess - asEt));
}

/** The report URLs for the last LOOKBACK_MARKS quarter-hours, newest first. */
function candidateUrls(now = new Date()) {
  const out = [];
  const q15 = 15 * 60 * 1000;
  let t = Math.floor(now.getTime() / q15) * q15;
  for (let i = 0; i < LOOKBACK_MARKS; i++, t -= q15) {
    const p = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23",
      year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })
      .formatToParts(new Date(t)).reduce((o, x) => ((o[x.type] = x.value), o), {});
    const h24 = +p.hour, ampm = h24 >= 12 ? "PM" : "AM", h12 = String(((h24 + 11) % 12) + 1).padStart(2, "0");
    out.push({
      url: `${BASE}${p.year}-${p.month}-${p.day}_${h12}_${p.minute}${ampm}.pdf`,
      reportAt: etToUtc(+p.year, +p.month, +p.day, h24, +p.minute),
    });
  }
  return out;
}

// Column left edges (PDF points) measured on the 2026-27 layout.
const COLS = [
  ["date", 0], ["time", 110], ["matchup", 195], ["team", 255],
  ["player", 420], ["status", 580], ["reason", 660],
];
const colOf = (x) => { let c = COLS[0][0]; for (const [name, left] of COLS) if (x >= left) c = name; return c; };
const STATUSES = new Set(["Out", "Doubtful", "Questionable", "Probable", "Available"]);

/** Parse one report PDF into [{ gameDate, matchup, team, player, status, reason }]. */
async function parseReport(buf) {
  const doc = await pdfjs.getDocument({ data: new Uint8Array(buf), verbosity: 0 }).promise;
  const rows = [];
  const carry = { date: null, matchup: null, team: null };
  for (let pn = 1; pn <= doc.numPages; pn++) {
    const page = await doc.getPage(pn);
    const items = (await page.getTextContent()).items
      .filter((i) => i.str && i.str.trim())
      .map((i) => ({ s: i.str.trim(), x: i.transform[4], y: i.transform[5] }));
    // Group into lines (same y within 2pt), top to bottom.
    items.sort((a, b) => b.y - a.y || a.x - b.x);
    const lines = [];
    for (const it of items) {
      const line = lines.find((l) => Math.abs(l.y - it.y) <= 2);
      if (line) line.items.push(it); else lines.push({ y: it.y, items: [it] });
    }
    for (const line of lines) {
      const cells = {};
      for (const it of line.items.sort((a, b) => a.x - b.x)) {
        const c = colOf(it.x);
        cells[c] = cells[c] ? `${cells[c]} ${it.s}` : it.s;
      }
      if (cells.player === "Player Name" || /^Page \d/.test(cells.reason || "") || /Injury Report:/.test(Object.values(cells).join(" "))) continue;
      if (cells.date && /^\d{2}\/\d{2}\/\d{4}$/.test(cells.date)) carry.date = cells.date;
      if (cells.matchup && /@/.test(cells.matchup)) carry.matchup = cells.matchup;
      if (cells.team) carry.team = cells.team;
      const status = cells.status && cells.status.split(" ")[0];
      if (cells.player && status && STATUSES.has(status) && carry.date && carry.team) {
        const [last, first] = cells.player.split(",").map((s) => s.trim());
        rows.push({
          gameDate: carry.date, matchup: carry.matchup, team: carry.team,
          player: first ? `${first} ${last}` : last, status, reason: cells.reason || null,
        });
      }
      // A reason that wraps puts its extra lines above and below the player's
      // line, so stitching them on attached text to the wrong player. Only the
      // player's own line is kept -- the status is what the timing test needs.
    }
  }
  return rows;
}

const isoDate = (mmddyyyy) => { const [m, d, y] = mmddyyyy.split("/"); return `${y}-${m}-${d}`; };

/** One pass: read any unread reports from the last two hours, log status changes. */
async function recordOfficialInjuries(now = new Date()) {
  const cands = candidateUrls(now);
  const { rows: seen } = await q(
    `select report_url from sports.nba_official_injury_reports where report_url = any($1)`,
    [cands.map((c) => c.url)]);
  const done = new Set(seen.map((r) => r.report_url));
  const todo = cands.filter((c) => !done.has(c.url)).reverse(); // oldest first
  let read = 0, changes = 0;
  for (const c of todo) {
    let res;
    try { res = await fetch(c.url, { signal: AbortSignal.timeout(20000) }); } catch (e) { log.warn(`official injuries: ${c.url}: ${e.message}`); continue; }
    if (res.status !== 200) continue; // not published (yet), or no report at this mark
    const parsed = await parseReport(Buffer.from(await res.arrayBuffer()));
    // Latest logged status per (game, team, player) for the dates in this report.
    const dates = [...new Set(parsed.map((r) => isoDate(r.gameDate)))];
    const { rows: last } = await q(
      `select distinct on (game_date, team, player) game_date::text gd, team, player, status
         from sports.nba_official_injuries where game_date = any($1::date[])
        order by game_date, team, player, first_seen_at desc`, [dates]);
    const prev = new Map(last.map((r) => [`${r.gd}|${r.team}|${r.player}`, r.status]));
    const fresh = parsed.filter((r) => prev.get(`${isoDate(r.gameDate)}|${r.team}|${r.player}`) !== r.status);
    for (const r of fresh) {
      await q(
        `insert into sports.nba_official_injuries
           (game_date, matchup, team, player, status, reason, report_at, report_url)
         values ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [isoDate(r.gameDate), r.matchup, r.team, r.player, r.status, r.reason, c.reportAt, c.url]);
    }
    await q(
      `insert into sports.nba_official_injury_reports (report_url, report_at, rows_parsed, changes)
       values ($1,$2,$3,$4) on conflict (report_url) do nothing`,
      [c.url, c.reportAt, parsed.length, fresh.length]);
    read++; changes += fresh.length;
    log(`official injuries: ${c.url.split("_").slice(1).join("_")} -> ${parsed.length} rows, ${fresh.length} status change(s)`);
  }
  return { read, changes };
}

module.exports = { recordOfficialInjuries, parseReport, candidateUrls };
