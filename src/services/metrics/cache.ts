// src/services/metrics/cache.ts
type CacheEntry<T> = { exp: number; val: T };

const memCache = new Map<string, CacheEntry<any>>();

export function cacheKey(parts: Record<string, any>) {
  return Object.entries(parts)
    .map(([k, v]) => `${k}=${String(v)}`)
    .join("|");
}

export function cacheGet<T>(key: string): T | null {
  const hit = memCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.exp) {
    memCache.delete(key);
    return null;
  }
  return hit.val as T;
}

export function cacheSet<T>(key: string, val: T, ttlMs: number) {
  memCache.set(key, { exp: Date.now() + ttlMs, val });
}
