/**
 * emailTracking.ts — per-recipient attribution for Resend template emails.
 *
 * Every app link in our Resend templates carries static UTMs
 * (utm_source=email&utm_medium=email&utm_campaign=…&utm_content=<button>) plus
 * `bp_rid={{{RID}}}`. The frontend analytics captures both into the first
 * event's `metadata.entry`, so an email click maps back to the account even
 * when the user never signs in that session.
 *
 * RID is an opaque, non-reversible ref: the first 16 hex chars of md5(users.id).
 * md5 is used (not for security) because Postgres has it built in, so a click
 * resolves to a user with no extension:
 *
 *   SELECT u.email, a.*
 *   FROM analytics_events a
 *   JOIN users u ON left(md5(u.id), 16) = a.metadata->'entry'->>'bp_rid'
 *   WHERE a.metadata->'entry'->>'utm_medium' = 'email';
 *
 * Templates declare RID with fallback "none", so a send that omits it still
 * renders (and old/new backend + template versions can deploy in any order).
 * Only pass `variables` to a template that declares RID (currently the weekly
 * newsletter ES/EN); other sends use `tags` alone.
 */
import { createHash } from "crypto";

export function emailRecipientRef(userId: string | null | undefined): string {
  if (!userId) return "none";
  return createHash("md5").update(userId).digest("hex").slice(0, 16);
}

/** Resend tag values allow only [A-Za-z0-9_-], max 256 chars. */
function tagValue(v: string): string {
  return v.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 256) || "none";
}

/**
 * Template variables + Resend tags for one recipient. Tags surface in the
 * Resend dashboard and on webhook events (email.clicked etc.), keyed by
 * campaign / language / recipient ref.
 */
export function emailTracking(opts: { userId: string | null | undefined; campaign: string; lang: string }) {
  const rid = emailRecipientRef(opts.userId);
  return {
    variables: { RID: rid },
    tags: [
      { name: "campaign", value: tagValue(opts.campaign) },
      { name: "lang", value: tagValue(opts.lang.toLowerCase()) },
      { name: "rid", value: rid },
    ],
  };
}
