// trading-bot/src/config.js
//
// Config + env loader. The active config file is re-read on every access so a
// live tuning edit (a weight, the delta cap) applies on the next tick without a
// restart -- same ergonomics as seed-bot.config.json.
//
// TWO BOTS, ONE CODEBASE. config.json is Adam-7 (moneylines) and
// config.argo-7.json is Argo-7 (totals). They share the feature store, the
// depth recorder, the paper filler and the NAV accounting, so the alternative
// -- a second copy of the folder -- would have duplicated ~1500 lines and
// doubled every future fix. Which config is active is chosen by
// TRADING_BOT_CONFIG (systemd sets it per unit) or by selectConfig() at the top
// of a run script.
//
// cfg() is read lazily inside functions everywhere, never at module scope, so
// selectConfig() called from a run script's first lines still takes effect for
// modules that were already required.

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../.env") });

const CONFIG_DIR = path.join(__dirname, "..");
const DEFAULT_CONFIG = "config.json";

let _active = process.env.TRADING_BOT_CONFIG || DEFAULT_CONFIG;
let _cache = null;
let _mtime = 0;
let _cachedPath = null;

/**
 * Point the loader at a different config file. Refuses anything outside the
 * bot directory: the file is parsed and then trusted completely, so a path
 * that could escape the folder is a wider hole than it looks.
 */
function selectConfig(fileName) {
  const resolved = path.resolve(CONFIG_DIR, fileName);
  if (path.dirname(resolved) !== path.resolve(CONFIG_DIR)) {
    throw new Error(`config must live in the bot directory: ${fileName}`);
  }
  if (!fs.existsSync(resolved)) throw new Error(`no such config: ${fileName}`);
  _active = fileName;
  _cache = null;
  _mtime = 0;
  return resolved;
}

const configPath = () => path.resolve(CONFIG_DIR, _active);

/** Re-reads the active config when it changes on disk, or when it changes. */
function cfg() {
  const p = configPath();
  const st = fs.statSync(p);
  if (!_cache || st.mtimeMs !== _mtime || p !== _cachedPath) {
    _cache = JSON.parse(fs.readFileSync(p, "utf8"));
    _mtime = st.mtimeMs;
    _cachedPath = p;
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

module.exports = { cfg, ENV, requireDb, selectConfig, configPath, CONFIG_DIR };
// Kept for the existing callers that import CONFIG_PATH as a constant.
Object.defineProperty(module.exports, "CONFIG_PATH", { get: configPath });
