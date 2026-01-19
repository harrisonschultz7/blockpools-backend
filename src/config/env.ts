// src/config/env.ts
import "dotenv/config";

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function opt(name: string, dflt?: string): string | undefined {
  const v = process.env[name];
  return v == null || v === "" ? dflt : v;
}

function num(name: string, dflt: number): number {
  const raw = opt(name);
  if (!raw) return dflt;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`Env var ${name} must be a number`);
  return n;
}

function bool(name: string, dflt: boolean): boolean {
  const raw = opt(name);
  if (raw == null) return dflt;
  return raw === "1" || raw.toLowerCase() === "true" || raw.toLowerCase() === "yes";
}

function csv(name: string): string[] {
  const raw = opt(name, "");
  return (raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export const ENV = {
  NODE_ENV: opt("NODE_ENV", "development")!,
  PORT: num("PORT", 8080),

  // Postgres
  DATABASE_URL: must("DATABASE_URL"),

  // CORS
  // Example: https://www.blockpools.io,https://blockpools.io,http://localhost:5173
  CORS_ORIGINS: csv("CORS_ORIGINS"),

  // Paging guards
  MIN_PAGE_SIZE: num("MIN_PAGE_SIZE", 5),
  MAX_PAGE_SIZE: num("MAX_PAGE_SIZE", 100),

  // Subgraph
  SUBGRAPH_QUERY_URL: opt("SUBGRAPH_QUERY_URL"), // optional (but leaderboard refresh will fail if missing)
  // If your subgraph needs auth, set this to something like:
  //   Bearer <token>
  // and we will send: Authorization: <SUBGRAPH_AUTH_HEADER>
  SUBGRAPH_AUTH_HEADER: opt("SUBGRAPH_AUTH_HEADER"),

  // Cache behavior
  CACHE_TTL_SECONDS: num("CACHE_TTL_SECONDS", 60), // "fresh" window
  CACHE_STALE_SECONDS: num("CACHE_STALE_SECONDS", 300), // serve stale for up to this age
  CACHE_REVALIDATE_SECONDS: num("CACHE_REVALIDATE_SECONDS", 30), // debounce revalidations

  // Worker
  CACHE_WORKER_ENABLED: bool("CACHE_WORKER_ENABLED", false),
  CACHE_WORKER_INTERVAL_SECONDS: num("CACHE_WORKER_INTERVAL_SECONDS", 60),
} as const;
