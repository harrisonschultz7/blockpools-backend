import { pg } from "../db/pg";

export type CacheRow = {
  cache_key: string;
  view: string;
  scope: string;
  params: any;
  payload: any | null;
  source_block: number | null;
  last_ok_at: string | null;
  expires_at: string | null;
  last_err_at: string | null;
  last_err: string | null;
};

export async function getCache(cacheKey: string): Promise<CacheRow | null> {
  const r = await pg.query(
    `select cache_key, view, scope, params, payload, source_block,
            last_ok_at, expires_at, last_err_at, last_err
       from public.cache_snapshots
      where cache_key = $1
      limit 1`,
    [cacheKey]
  );
  return r.rows[0] || null;
}

export async function upsertOk(args: {
  cacheKey: string;
  view: string;
  scope: "global" | "user";
  params: any;
  payload: any;
  sourceBlock: number | null;
  ttlSeconds: number;
}) {
  const expiresAt = new Date(Date.now() + args.ttlSeconds * 1000).toISOString();

  await pg.query(
    `insert into public.cache_snapshots
      (cache_key, view, scope, params, payload, source_block, last_ok_at, expires_at, last_err_at, last_err)
     values
      ($1, $2, $3, $4::jsonb, $5::jsonb, $6, now(), $7, null, null)
     on conflict (cache_key) do update set
      view = excluded.view,
      scope = excluded.scope,
      params = excluded.params,
      payload = excluded.payload,
      source_block = excluded.source_block,
      last_ok_at = now(),
      expires_at = excluded.expires_at,
      last_err_at = null,
      last_err = null`,
    [
      args.cacheKey,
      args.view,
      args.scope,
      JSON.stringify(args.params),
      JSON.stringify(args.payload),
      args.sourceBlock,
      expiresAt,
    ]
  );
}

export async function upsertErr(args: {
  cacheKey: string;
  view: string;
  scope: "global" | "user";
  params: any;
  err: string;
}) {
  await pg.query(
    `insert into public.cache_snapshots
      (cache_key, view, scope, params, payload, source_block, last_ok_at, expires_at, last_err_at, last_err)
     values
      ($1, $2, $3, $4::jsonb, null, null, null, null, now(), $5)
     on conflict (cache_key) do update set
      view = excluded.view,
      scope = excluded.scope,
      params = excluded.params,
      last_err_at = now(),
      last_err = excluded.last_err`,
    [args.cacheKey, args.view, args.scope, JSON.stringify(args.params), args.err.slice(0, 2000)]
  );
}

export function isFresh(row: CacheRow | null) {
  if (!row?.payload || !row.expires_at) return false;
  return Date.now() < new Date(row.expires_at).getTime();
}

export function cacheMeta(row: CacheRow | null) {
  const lastOkMs = row?.last_ok_at ? new Date(row.last_ok_at).getTime() : 0;
  const ageSeconds = lastOkMs ? Math.floor((Date.now() - lastOkMs) / 1000) : null;
  const stale = !isFresh(row);
  return {
    stale,
    lastOkAt: row?.last_ok_at ?? null,
    ageSeconds,
    sourceBlock: row?.source_block ?? null,
    lastErrAt: row?.last_err_at ?? null,
    lastErr: row?.last_err ?? null,
  };
}
