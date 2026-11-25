// src/db.ts
import { Pool } from "pg";
import { API_PG_IDLE_TIMEOUT_MS, apiDatabaseUrl } from "./db/poolerUrl";

// Session-pooler URLs go to the transaction pooler; see db/poolerUrl.ts.
const connectionString = apiDatabaseUrl(process.env.DATABASE_URL || "");
if (!connectionString) {
  throw new Error("DATABASE_URL is not set in environment");
}

/**
 * Supabase typically requires SSL. For local Postgres you can set:
 *   DATABASE_SSL=false
 */
const DATABASE_SSL = (process.env.DATABASE_SSL || "true").toLowerCase() !== "false";

export const pool = new Pool({
  connectionString,
  ssl: DATABASE_SSL
    ? {
        rejectUnauthorized: false,
      }
    : undefined,

  // Sensible defaults for a small VPS
  max: Number(process.env.PG_POOL_MAX || 10),
  // Kept 10 minutes (was 30s) so a quiet API isn't reconnecting all day.
  idleTimeoutMillis: API_PG_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10_000),
  keepAlive: true,
});

// Optional: surface unexpected idle client errors
pool.on("error", (err) => {
  console.error("[db] Unexpected error on idle client", err);
});
