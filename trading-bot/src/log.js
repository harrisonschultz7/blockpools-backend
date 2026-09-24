// trading-bot/src/log.js
// Synchronous stdout so journald shows logs live rather than only on exit --
// same fix seed-bot.js needed (Node buffers piped, non-TTY stdout).
try {
  process.stdout._handle && process.stdout._handle.setBlocking &&
    process.stdout._handle.setBlocking(true);
} catch {}

const stamp = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const log = (...a) => console.log(`[${stamp()}]`, ...a);
log.warn = (...a) => console.log(`[${stamp()}] WARN`, ...a);
log.err = (...a) => console.error(`[${stamp()}] ERROR`, ...a);

module.exports = log;
