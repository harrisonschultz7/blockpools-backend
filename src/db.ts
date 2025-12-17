// src/db.ts
import { Pool } from "pg";

const connectionString = (process.env.DATABASE_URL || "").trim();
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
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
  connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10_000),
});

// Optional: surface unexpected idle client errors
pool.on("error", (err) => {
  console.error("[db] Unexpected error on idle client", err);
});
