// src/db/pg.ts
import { Pool } from "pg";
import { ENV } from "../config/env";
import { API_PG_IDLE_TIMEOUT_MS, apiDatabaseUrl } from "./poolerUrl";

// Same rules as src/db.ts: transaction pooler, connections kept 10 minutes.
export const pg = new Pool({
  connectionString: apiDatabaseUrl(ENV.DATABASE_URL),
  max: 10,
  idleTimeoutMillis: API_PG_IDLE_TIMEOUT_MS,
  keepAlive: true,
});
pg.on("error", (err) => {
  console.error("[db/pg] idle client error:", err?.message);
});

export async function pingDb(): Promise<boolean> {
  const r = await pg.query("select 1 as ok");
  return r.rows?.[0]?.ok === 1;
}
