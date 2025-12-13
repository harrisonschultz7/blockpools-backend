// src/services/emailService.ts
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

export async function sendTestEmail(to: string) {
  requireEnv("RESEND_API_KEY");
  const fromInvites = requireEnv("EMAIL_FROM_INVITES");

  return resend.emails.send({
    from: fromInvites,
    to,
    subject: "BlockPools Resend Test (Invites)",
    html: `
      <div style="font-family: ui-sans-serif, system-ui; line-height: 1.5;">
        <h2>Resend test successful</h2>
        <p>This confirms your backend can send via <b>invites@mail.blockpools.io</b>.</p>
      </div>
    `,
  });
}

export async function sendWelcomeEmail(opts: { to: string; username: string }) {
  requireEnv("RESEND_API_KEY");
  const fromWelcome = requireEnv("EMAIL_FROM_WELCOME");

  return resend.emails.send({
    from: fromWelcome,
    to: opts.to,
    subject: `Welcome to BlockPools, ${opts.username}`,
    html: `
      <div style="font-family: ui-sans-serif, system-ui; line-height: 1.5;">
        <h2>Welcome to BlockPools</h2>
        <p>Profile created for <b>${escapeHtml(opts.username)}</b>.</p>
        <p>You can now invite friends and start trading.</p>
      </div>
    `,
  });
}

// Minimal HTML escape for username (prevents HTML injection in emails)
function escapeHtml(input: string) {
  return input
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
