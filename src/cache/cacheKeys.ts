import crypto from "crypto";

/**
 * Cache keys must be stable across param-object order, and resilient to
 * minor naming differences (address vs user, etc).
 */
function stableJson(obj: any) {
  if (!obj || typeof obj !== "object") return JSON.stringify(obj);

  const keys = Object.keys(obj).sort();
  const out: any = {};
  for (const k of keys) out[k] = obj[k];
  return JSON.stringify(out);
}

function hashParams(params: any) {
  return crypto
    .createHash("sha256")
    .update(stableJson(params))
    .digest("hex")
    .slice(0, 24);
}

export function keyLeaderboard(params: any) {
  return `leaderboard:global:${hashParams(params)}`;
}

export function keyUserSummary(params: any) {
  // supports { address, first } or { user, betsFirst, ... } etc.
  return `userSummary:user:${hashParams(params)}`;
}

export function keyUserBetsPage(params: any) {
  return `userBetsPage:user:${hashParams(params)}`;
}

/**
 * NEW: user trades (BUY + SELL) page cache key
 * Params should include: { user, leagues, range, page, pageSize }
 *
 * NOTE: Versioned to invalidate old cached payloads when merge/dedupe logic changes.
 */
export function keyUserTradesPage(params: any) {
  return `userTradesPage_v2:user:${hashParams(params)}`;
}

export function keyUserClaimsAndStats(params: any) {
  return `userClaimsAndStats:user:${hashParams(params)}`;
}
