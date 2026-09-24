// trading-bot/src/config.js
//
// Config + env loader. config.json is re-read on every access so a live tuning
// edit (a weight, the delta cap) applies on the next tick without a restart --
// same ergonomics as seed-bot.config.json.

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../.env") });

const CONFIG_PATH = path.join(__dirname, "../config.json");

let _cache = null;
let _mtime = 0;

/** Re-reads config.json when it changes on disk. */
function cfg() {
  const st = fs.statSync(CONFIG_PATH);
  if (!_cache || st.mtimeMs !== _mtime) {
    _cache = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
    _mtime = st.mtimeMs;
  }
  return _cache;
}

const ENV = {
  // The shared DATABASE_URL points at Supabase's DIRECT host, which resolves
  // to IPv6 only. Fine on the VPS, but unreachable from an IPv4-only dev box,
  // so allow a pooler override for local runs rather than editing the URL the
  // rest of the backend depends on.
  DATABASE_URL: process.env.TRADING_BOT_DATABASE_URL || process.env.DATABASE_URL || "",
  // Public, no-auth read endpoints. The bot never signs anything in paper mode.
  GAMMA_API_URL: process.env.GAMMA_API_URL || "https://gamma-api.polymarket.com",
  CLOB_API_URL: process.env.CLOB_API_URL || "https://clob.polymarket.com",
  NFLVERSE_BASE:
    process.env.NFLVERSE_BASE ||
    "https://github.com/nflverse/nflverse-data/releases/download",
  OPEN_METEO_URL: process.env.OPEN_METEO_URL || "https://api.open-meteo.com/v1/forecast",
  ESPN_NFL_URL:
    process.env.ESPN_NFL_URL ||
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl",
  LOG_LEVEL: process.env.TRADING_BOT_LOG_LEVEL || "info",
};

function requireDb() {
  if (!ENV.DATABASE_URL) {
    throw new Error(
      "No database URL. Set DATABASE_URL in the backend .env, or " +
      "TRADING_BOT_DATABASE_URL to a pooler connection string for local runs.",
    );
  }
  return ENV.DATABASE_URL;
}

module.exports = { cfg, ENV, requireDb, CONFIG_PATH };
