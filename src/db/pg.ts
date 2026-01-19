// src/db/pg.ts
import { Pool } from "pg";
import { ENV } from "../config/env";

export const pg = new Pool({
  connectionString: ENV.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30_000,
});

export async function pingDb(): Promise<boolean> {
  const r = await pg.query("select 1 as ok");
  return r.rows?.[0]?.ok === 1;
}
