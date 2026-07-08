// src/services/attribution.ts
//
// First-touch ad attribution for users.
//
// UTM params land on an anonymous visitor_id/session_id BEFORE the wallet
// connects (see frontend src/lib/analytics.ts `entryContext`). They are never
// written to the user directly. The only durable link from an ad click to a
// user is the visitor_id bridge: the same browser (visitor_id) that carried the
// UTM later carries the connected wallet_address on its analytics rows.
//
// resolveAttribution() walks that bridge for a single wallet and writes the
// earliest (first-touch) UTM onto public.users — but ONLY when attributed_at is
// still NULL, so first-touch always wins and re-runs are no-ops. It is therefore
// safe to call opportunistically (on new-user insert, on first trade, etc.).

import { pool } from "../db";

export interface Attribution {
  utm_source: string | null;
  utm_campaign: string | null;
  utm_content: string | null; // Meta ad id
  utm_term: string | null;
  landing: string | null;
  attributed_at: string | null;
}

/**
 * Resolve and persist first-touch attribution for one wallet address.
 *
 * Returns the attribution now on the user row (whether newly written by this
 * call or already present), or null if the user row / any attribution could not
 * be found. Never throws — attribution must never break a request or a trade.
 */
export async function resolveAttribution(
  address: string | null | undefined
): Promise<Attribution | null> {
  if (!address) return null;
  const addr = address.toLowerCase();

  try {
    // Fill attribution from the visitor bridge, but only if not already set.
    // First-touch = earliest entry-with-utm across every visitor_id ever seen
    // carrying this wallet. The UTM row itself may have a NULL wallet_address
    // (it landed pre-connect) — that's expected, we match on visitor_id.
    const filled = await pool.query(
      `
      WITH visitors AS (
        SELECT DISTINCT visitor_id
        FROM public.analytics_events
        WHERE visitor_id IS NOT NULL
          AND lower(wallet_address) = $1
      ),
      first_touch AS (
        SELECT ae.metadata->'entry' AS entry, ae.created_at
        FROM public.analytics_events ae
        WHERE ae.visitor_id IN (SELECT visitor_id FROM visitors)
          AND ae.metadata->'entry'->>'utm_source' IS NOT NULL
        ORDER BY ae.created_at ASC
        LIMIT 1
      )
      UPDATE public.users u
      SET attributed_utm_source   = ft.entry->>'utm_source',
          attributed_utm_campaign = ft.entry->>'utm_campaign',
          attributed_utm_content  = ft.entry->>'utm_content',
          attributed_utm_term     = ft.entry->>'utm_term',
          attributed_landing      = ft.entry->>'landing',
          attributed_at           = ft.created_at
      FROM first_touch ft
      WHERE lower(u.primary_address) = $1
        AND u.attributed_at IS NULL
      RETURNING
        attributed_utm_source, attributed_utm_campaign, attributed_utm_content,
        attributed_utm_term, attributed_landing, attributed_at
      `,
      [addr]
    );

    if (filled.rows.length > 0) {
      return rowToAttribution(filled.rows[0]);
    }

    // Nothing written (already attributed, or no bridge match). Return whatever
    // is already on the row so callers (e.g. FirstTrade) can read the ad id.
    const existing = await pool.query(
      `
      SELECT attributed_utm_source, attributed_utm_campaign, attributed_utm_content,
             attributed_utm_term, attributed_landing, attributed_at
      FROM public.users
      WHERE lower(primary_address) = $1
      LIMIT 1
      `,
      [addr]
    );
    if (existing.rows.length > 0 && existing.rows[0].attributed_at) {
      return rowToAttribution(existing.rows[0]);
    }
    return null;
  } catch (err) {
    console.error("[attribution] resolveAttribution failed", err);
    return null;
  }
}

function rowToAttribution(r: any): Attribution {
  return {
    utm_source: r.attributed_utm_source ?? null,
    utm_campaign: r.attributed_utm_campaign ?? null,
    utm_content: r.attributed_utm_content ?? null,
    utm_term: r.attributed_utm_term ?? null,
    landing: r.attributed_landing ?? null,
    attributed_at: r.attributed_at
      ? new Date(r.attributed_at).toISOString()
      : null,
  };
}
