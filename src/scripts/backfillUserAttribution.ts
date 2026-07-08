// src/scripts/backfillUserAttribution.ts
//
// ONE-TIME (re-runnable) backfill of first-touch ad attribution onto existing
// users, recovered via the visitor_id -> wallet bridge in analytics_events.
//
// For every user with a primary_address, find the earliest entry-with-utm across
// all visitor_ids ever seen carrying that wallet, and write attributed_utm_* +
// attributed_landing + attributed_at — but ONLY where attributed_at IS NULL, so
// it never overwrites a first-touch and is safe to re-run (e.g. after more
// bridge data accrues). Expected coverage is modest (~25% of users) — that's the
// nature of the bridge; it recovers exactly what survived.
//
// Requires the 2026-07-08_users_attribution.sql migration to be applied first.
//
// Run (after `npm run build`):
//   node dist/scripts/backfillUserAttribution.js            # apply
//   DRY_RUN=1 node dist/scripts/backfillUserAttribution.js  # preview counts only

import { pool } from "../db";

// The core CTE is shared by the dry-run preview and the UPDATE so they can never
// diverge. Produces one first-touch row per wallet (wa) that has bridge UTM.
const FIRST_TOUCH_CTE = `
  WITH wallet_visitors AS (
    SELECT DISTINCT lower(wallet_address) AS wa, visitor_id
    FROM public.analytics_events
    WHERE wallet_address IS NOT NULL AND visitor_id IS NOT NULL
  ),
  utm_rows AS (
    SELECT visitor_id, metadata->'entry' AS entry, created_at
    FROM public.analytics_events
    WHERE visitor_id IS NOT NULL
      AND metadata->'entry'->>'utm_source' IS NOT NULL
  ),
  ranked AS (
    SELECT wv.wa, ur.entry, ur.created_at,
           row_number() OVER (PARTITION BY wv.wa ORDER BY ur.created_at ASC) AS rn
    FROM wallet_visitors wv
    JOIN utm_rows ur ON ur.visitor_id = wv.visitor_id
  ),
  first_touch AS (
    SELECT wa, entry, created_at FROM ranked WHERE rn = 1
  )
`;

async function main() {
  const dryRun = /^(1|true|yes)$/i.test((process.env.DRY_RUN || "").trim());

  if (dryRun) {
    const { rows } = await pool.query(
      `${FIRST_TOUCH_CTE}
       SELECT
         (SELECT count(*) FROM public.users WHERE primary_address IS NOT NULL) AS users_with_addr,
         (SELECT count(*) FROM public.users WHERE attributed_at IS NOT NULL)    AS already_attributed,
         count(*) FILTER (
           WHERE EXISTS (
             SELECT 1 FROM public.users u
             WHERE lower(u.primary_address) = ft.wa AND u.attributed_at IS NULL
           )
         ) AS would_fill
       FROM first_touch ft`
    );
    const r = rows[0] || {};
    console.log("[backfillUserAttribution] DRY RUN — no writes");
    console.log(`  users with primary_address : ${r.users_with_addr}`);
    console.log(`  already attributed         : ${r.already_attributed}`);
    console.log(`  would fill this run        : ${r.would_fill}`);
    await pool.end();
    return;
  }

  const { rowCount } = await pool.query(
    `${FIRST_TOUCH_CTE}
     UPDATE public.users u
     SET attributed_utm_source   = ft.entry->>'utm_source',
         attributed_utm_campaign = ft.entry->>'utm_campaign',
         attributed_utm_content  = ft.entry->>'utm_content',
         attributed_utm_term     = ft.entry->>'utm_term',
         attributed_landing      = ft.entry->>'landing',
         attributed_at           = ft.created_at
     FROM first_touch ft
     WHERE lower(u.primary_address) = ft.wa
       AND u.attributed_at IS NULL`
  );

  console.log(`[backfillUserAttribution] attributed ${rowCount} user(s).`);
  await pool.end();
}

main().catch((err) => {
  console.error("[backfillUserAttribution] failed", err);
  process.exit(1);
});
