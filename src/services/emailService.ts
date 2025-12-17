// src/services/emailService.ts
import { Resend } from "resend";

/**
 * Throws if an env var is missing.
 */
function requireEnv(key: string): string {
  const v = process.env[key];
  if (!v || !v.trim()) {
    throw new Error(`Missing required env var: ${key}`);
  }
  return v.trim();
}

/**
 * Minimal HTML escaping for dynamic values inserted into email HTML.
 */
function escapeHtml(input: string): string {
  return input
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

const resend = new Resend(requireEnv("RESEND_API_KEY"));

export async function sendWelcomeEmail(opts: { to: string; username: string }) {
  const fromWelcome = requireEnv("EMAIL_FROM_WELCOME");
  const appBaseUrl = requireEnv("APP_BASE_URL").replace(/\/+$/, "");
  const username = escapeHtml(opts.username);

  return resend.emails.send({
    from: fromWelcome,
    to: opts.to,
    subject: `Welcome to BlockPools, ${username}`,
    html: `
      <div style="font-family: ui-sans-serif, system-ui; line-height: 1.5;">
        <h2>Welcome to BlockPools</h2>
        <p>Profile created for <b>${username}</b>.</p>
        <p>
          <a href="${appBaseUrl}/app"
             style="display:inline-block;padding:10px 14px;border-radius:10px;text-decoration:none;border:1px solid #e6d7b5">
            Open BlockPools
          </a>
        </p>
      </div>
    `,
  });
}

export async function sendTestEmail(opts: { to: string }) {
  const fromWelcome = requireEnv("EMAIL_FROM_WELCOME");
  return resend.emails.send({
    from: fromWelcome,
    to: opts.to,
    subject: "BlockPools email test",
    html: `<div style="font-family: ui-sans-serif, system-ui; line-height: 1.5;">
      <h3>Email delivery test</h3>
      <p>If you received this, Resend is working.</p>
    </div>`,
  });
}
