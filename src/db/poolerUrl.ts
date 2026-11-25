// src/db/poolerUrl.ts
//
// Connection settings shared by the API's two pg pools (src/db.ts, src/db/pg.ts).
//
// WHY (2026-11-25): the Supabase org went over the free plan's 5 GB/month
// egress, and most of it was connection churn, not query data. Both pools
// closed connections after 30s idle, so a quiet API reopened them all day:
// 80-530 new session-mode connections an hour in the supavisor logs, each a
// TLS handshake + startup. Keeping them open needs the TRANSACTION pooler
// (:6543): the session pooler (:5432) gives each idle client a server
// connection and is capped at 15 clients (exhausted once, 2026-10-18), so
// ten parked API connections there would starve the other services.
//
// The API uses nothing that needs a session: no LISTEN/NOTIFY, advisory
// locks, session SET, named prepared statements or temp tables, and its
// explicit transactions (BEGIN ... COMMIT) run on one checked-out client.
//
// So a Supabase pooler URL on :5432 is pointed at :6543. A direct db.*
// host, any other host, or DB_USE_SESSION_POOLER=true leaves it unchanged.

export function apiDatabaseUrl(raw: string): string {
  const url = (raw || "").trim();
  if (!url) return url;
  if (String(process.env.DB_USE_SESSION_POOLER || "").toLowerCase() === "true") return url;
  // Only the port is touched (string replace, not a URL round-trip), so the
  // password's encoding can't change.
  return url.replace(/(@[^/@?#]*\.pooler\.supabase\.com):5432(?=[/?#]|$)/i, "$1:6543");
}

/** Idle connections live this long (was 30s). PG_IDLE_TIMEOUT_MS overrides. */
export const API_PG_IDLE_TIMEOUT_MS = Number(process.env.PG_IDLE_TIMEOUT_MS || 600_000);
