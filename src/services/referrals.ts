// src/services/referrals.ts
//
// Referral attribution: who brought whom to BlockPools. Two ways in, one record:
//
//   email  POST /api/invites/email sends a single-use link (/app?invite=<64-hex
//          token>). Its invites row exists before the friend signs up.
//   link   every user has a reusable personal link (/invite/<referral_code>).
//          Each friend who signs up through it gets their OWN invites row,
//          inserted already 'redeemed' with kind='link'.
//
// Either way a referral is an invites row with status='redeemed' and
// redeemed_by_user_id = the friend. The rules, enforced in one transaction:
//   - a user is referred at most once (first referral wins);
//   - only a new account can be referred (no users row yet, or one created in
//     the last NEW_ACCOUNT_WINDOW_HOURS) — an existing user who clicks a
//     friend's link later doesn't move over to them;
//   - nobody refers themselves.
// Redeeming again with the same inviter is a no-op success, so the frontend's
// retries are safe.
//
// The credit itself (1% of the friend's trading) is paid out by hand from
// public.referral_credits() — see scripts/sql/2026-11-10_referral_links.sql.

import crypto from "crypto";
import type { Pool, PoolClient } from "pg";

export const NEW_ACCOUNT_WINDOW_HOURS = 24;

// No 0/O/1/I/L, so a code read aloud or typed from a screenshot still works.
const CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";
const CODE_LENGTH = 8;

const EMAIL_TOKEN_RE = /^[a-f0-9]{64}$/i;
const CODE_RE = /^[A-Z0-9]{6,16}$/;

export function sha256Hex(input: string) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

export function generateReferralCode(): string {
  const bytes = crypto.randomBytes(CODE_LENGTH);
  let out = "";
  for (let i = 0; i < CODE_LENGTH; i++) out += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
  return out;
}

/** The two token shapes /invites/redeem accepts, or null for anything else. */
export function parseInviteToken(raw: unknown):
  | { kind: "email"; token: string }
  | { kind: "link"; code: string }
  | null {
  const s = String(raw ?? "").trim();
  if (EMAIL_TOKEN_RE.test(s)) return { kind: "email", token: s.toLowerCase() };
  const code = s.toUpperCase();
  if (CODE_RE.test(code)) return { kind: "link", code };
  return null;
}

type Db = Pick<Pool, "query" | "connect">;

/**
 * The user's personal referral code, created on first ask. Null when the user
 * has no users row yet (no profile), since the code lives on that row.
 */
export async function getOrCreateReferralCode(db: Db, userId: string): Promise<string | null> {
  for (let attempt = 0; attempt < 6; attempt++) {
    const { rows } = await db.query(`select referral_code from public.users where id = $1`, [userId]);
    if (!rows[0]) return null;
    if (rows[0].referral_code) return rows[0].referral_code as string;
    try {
      const upd = await db.query(
        `update public.users set referral_code = $2
          where id = $1 and referral_code is null
        returning referral_code`,
        [userId, generateReferralCode()]
      );
      if (upd.rows[0]) return upd.rows[0].referral_code as string;
      // Lost a race with a concurrent request: loop and read theirs.
    } catch (e: any) {
      if (e?.code !== "23505") throw e; // code collision: try another
    }
  }
  throw new Error("Could not assign a referral code");
}

export async function countReferrals(db: Db, userId: string): Promise<number> {
  const { rows } = await db.query(
    `select count(distinct redeemed_by_user_id)::int as n
       from public.invites
      where inviter_user_id = $1
        and status = 'redeemed'
        and redeemed_by_user_id is not null
        and redeemed_by_user_id <> inviter_user_id`,
    [userId]
  );
  return rows[0]?.n ?? 0;
}

export type RedeemOutcome =
  | { ok: true; inviteId: number; inviterUserId: string; kind: "email" | "link"; alreadyRedeemed: boolean }
  | {
      ok: false;
      reason: "bad_token" | "not_found" | "self_referral" | "existing_user" | "already_referred" | "already_used";
    };

/** Attribute `refereeUserId` to whoever owns `rawToken`. See the rules up top. */
export async function redeemReferral(
  db: Db,
  rawToken: unknown,
  refereeUserId: string
): Promise<RedeemOutcome> {
  const parsed = parseInviteToken(rawToken);
  if (!parsed) return { ok: false, reason: "bad_token" };

  const client: PoolClient = await db.connect();
  try {
    await client.query("BEGIN");
    // Serialize redemptions per friend so two tabs/retries can't both win.
    await client.query(`select pg_advisory_xact_lock(hashtext($1))`, [`referral:${refereeUserId}`]);

    // 1. Whose invite is it?
    let inviterUserId: string;
    let emailInviteId: number | null = null;
    if (parsed.kind === "email") {
      const { rows } = await client.query(
        `select id, inviter_user_id from public.invites where token_hash = $1 for update`,
        [sha256Hex(parsed.token)]
      );
      if (!rows[0]) return await finish(client, { ok: false, reason: "not_found" });
      inviterUserId = rows[0].inviter_user_id;
      emailInviteId = Number(rows[0].id);
    } else {
      const { rows } = await client.query(`select id from public.users where referral_code = $1`, [parsed.code]);
      if (!rows[0]) return await finish(client, { ok: false, reason: "not_found" });
      inviterUserId = rows[0].id;
    }

    if (inviterUserId === refereeUserId) return await finish(client, { ok: false, reason: "self_referral" });

    // 2. Already referred? Same inviter again is a harmless retry.
    const prior = await client.query(
      `select id, inviter_user_id, kind from public.invites
        where status = 'redeemed' and redeemed_by_user_id = $1
        order by redeemed_at asc nulls last, id asc
        limit 1`,
      [refereeUserId]
    );
    if (prior.rows[0]) {
      if (prior.rows[0].inviter_user_id === inviterUserId) {
        return await finish(client, {
          ok: true,
          inviteId: Number(prior.rows[0].id),
          inviterUserId,
          kind: prior.rows[0].kind,
          alreadyRedeemed: true,
        });
      }
      return await finish(client, { ok: false, reason: "already_referred" });
    }

    // 3. Only new accounts can be referred.
    const acct = await client.query(
      `select (created_at < now() - make_interval(hours => $2)) as is_old
         from public.users where id = $1`,
      [refereeUserId, NEW_ACCOUNT_WINDOW_HOURS]
    );
    if (acct.rows[0]?.is_old) return await finish(client, { ok: false, reason: "existing_user" });

    // 4. Record it.
    if (emailInviteId != null) {
      const { rows } = await client.query(
        `update public.invites
            set status = 'redeemed', redeemed_at = now(), redeemed_by_user_id = $2
          where id = $1 and status in ('sent', 'pending', 'accepted')
        returning id`,
        [emailInviteId, refereeUserId]
      );
      if (!rows[0]) return await finish(client, { ok: false, reason: "already_used" });
      return await finish(client, {
        ok: true, inviteId: emailInviteId, inviterUserId, kind: "email", alreadyRedeemed: false,
      });
    }

    const code = (parsed as { code: string }).code;
    const { rows } = await client.query(
      `insert into public.invites
         (inviter_user_id, invitee_email, token_hash, status, kind, redeemed_at, redeemed_by_user_id)
       values ($1, null, $2, 'redeemed', 'link', now(), $3)
       returning id`,
      [inviterUserId, sha256Hex(`link:${code}:${refereeUserId}`), refereeUserId]
    );
    return await finish(client, {
      ok: true, inviteId: Number(rows[0].id), inviterUserId, kind: "link", alreadyRedeemed: false,
    });
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

async function finish(client: PoolClient, out: RedeemOutcome): Promise<RedeemOutcome> {
  await client.query(out.ok ? "COMMIT" : "ROLLBACK");
  return out;
}
