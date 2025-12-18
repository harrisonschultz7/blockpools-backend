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

type EmailButton = {
  label: string;
  href: string;
};

type BrandedEmailOpts = {
  title: string;
  preheader?: string;
  intro?: string; // small paragraph under title
  button?: EmailButton;
  footerNote?: string;
  bodyHtml?: string; // for custom blocks
};

/**
 * Email clients are inconsistent; table-based layout is still the safest.
 * This template is dark UI with “BlockPools gold” accent.
 */
function renderBrandedEmail(opts: BrandedEmailOpts) {
  const title = escapeHtml(opts.title);
  const preheader = escapeHtml(opts.preheader || "");
  const intro = opts.intro ? escapeHtml(opts.intro) : "";
  const footerNote = opts.footerNote ? escapeHtml(opts.footerNote) : "";

  const buttonHtml = opts.button
    ? `
      <tr>
        <td align="center" style="padding: 18px 0 8px;">
          <a href="${opts.button.href}"
             style="
              display:inline-block;
              padding: 12px 18px;
              border-radius: 12px;
              text-decoration:none;
              font-weight: 700;
              letter-spacing: 0.2px;
              background: #facc15;
              color: #0b0f19;
              border: 1px solid rgba(250,204,21,0.45);
            ">
            ${escapeHtml(opts.button.label)}
          </a>
        </td>
      </tr>
    `
    : "";

  const customBody = opts.bodyHtml ? opts.bodyHtml : "";

  // Preheader: hidden text that shows in inbox preview
  const preheaderHtml = preheader
    ? `
      <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
        ${preheader}
      </div>
    `
    : "";

  return `
  <!doctype html>
  <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <meta name="color-scheme" content="dark" />
      <meta name="supported-color-schemes" content="dark" />
      <title>${title}</title>
    </head>
    <body style="margin:0;padding:0;background:#070a12;">
      ${preheaderHtml}

      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"
             style="background:#070a12;padding: 26px 12px;">
        <tr>
          <td align="center">
            <table role="presentation" width="640" cellspacing="0" cellpadding="0" border="0"
                   style="max-width:640px;width:100%;">
              <tr>
                <td style="
                  background: radial-gradient(circle at 0% 0%, rgba(250,204,21,0.18), rgba(7,10,18,0.2) 55%),
                              radial-gradient(circle at 100% 0%, rgba(255,255,255,0.06), rgba(7,10,18,0.2) 55%),
                              linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
                  border: 1px solid rgba(255,255,255,0.10);
                  border-radius: 18px;
                  overflow:hidden;
                ">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                    <tr>
                      <td style="padding: 18px 18px 0;">
                        <div style="
                          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                          color: rgba(255,255,255,0.92);
                          font-size: 14px;
                          letter-spacing: 0.8px;
                          text-transform: uppercase;
                        ">
                          BlockPools
                        </div>
                      </td>
                    </tr>

                    <tr>
                      <td style="padding: 10px 18px 0;">
                        <div style="
                          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                          color: rgba(255,255,255,0.96);
                          font-size: 22px;
                          font-weight: 800;
                          line-height: 1.2;
                        ">
                          ${title}
                        </div>
                      </td>
                    </tr>

                    ${
                      intro
                        ? `
                      <tr>
                        <td style="padding: 10px 18px 0;">
                          <div style="
                            font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                            color: rgba(255,255,255,0.82);
                            font-size: 14px;
                            line-height: 1.6;
                          ">
                            ${intro}
                          </div>
                        </td>
                      </tr>
                    `
                        : ""
                    }

                    ${customBody}

                    ${buttonHtml}

                    <tr>
                      <td style="padding: 14px 18px 18px;">
                        <div style="
                          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                          color: rgba(255,255,255,0.55);
                          font-size: 12px;
                          line-height: 1.5;
                        ">
                          ${footerNote || "If you weren’t expecting this, you can ignore this email."}
                        </div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <tr>
                <td align="center" style="padding: 14px 8px 0;">
                  <div style="
                    font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                    color: rgba(255,255,255,0.45);
                    font-size: 12px;
                    line-height: 1.4;
                  ">
                    © ${new Date().getFullYear()} BlockPools
                  </div>
                </td>
              </tr>

            </table>
          </td>
        </tr>
      </table>
    </body>
  </html>
  `;
}

export async function sendWelcomeEmail(opts: { to: string; username: string }) {
  const fromWelcome = requireEnv("EMAIL_FROM_WELCOME");
  const appBaseUrl = requireEnv("APP_BASE_URL").replace(/\/+$/, "");
  const username = escapeHtml(opts.username);

  return resend.emails.send({
    from: fromWelcome,
    to: opts.to,
    subject: `Welcome to BlockPools, ${username}`,
    html: renderBrandedEmail({
      title: `Welcome, ${username}`,
      preheader: "Your BlockPools profile is ready.",
      intro:
        "Your profile has been created. You can open BlockPools below and start exploring live markets.",
      button: { label: "Open BlockPools", href: `${appBaseUrl}/app` },
      footerNote: "If you did not create this account, you can ignore this email.",
    }),
  });
}

/**
 * Simple 3-line invite (plus optional fallback link).
 */
export async function sendInviteEmail(opts: {
  to: string;
  inviteUrl: string;
  inviterLabel?: string;
}) {
  const fromInvites = requireEnv("EMAIL_FROM_INVITES");

  const inviterRaw = (opts.inviterLabel || "").trim();
  const inviterName = inviterRaw ? escapeHtml(inviterRaw) : "A friend";

  const bodyHtml = `
    <tr>
      <td style="padding: 14px 18px 0;">
        <div style="
          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          color: rgba(255,255,255,0.96);
          font-size: 18px;
          font-weight: 800;
          line-height: 1.35;
        ">
          ${inviterName} invited you to BlockPools
        </div>
      </td>
    </tr>

    <tr>
      <td style="padding: 10px 18px 0;">
        <div style="
          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          color: rgba(255,255,255,0.78);
          font-size: 14px;
          line-height: 1.7;
        ">
          BlockPools turns sports predictions into ownership — fees flow into team-linked tokens.
        </div>
      </td>
    </tr>

    <tr>
      <td style="padding: 12px 18px 0;">
        <div style="
          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          color: rgba(255,255,255,0.55);
          font-size: 12px;
          line-height: 1.6;
        ">
          If the button doesn’t work, paste this link into your browser:<br/>
          <a href="${opts.inviteUrl}" style="
            color: rgba(250,204,21,0.95);
            text-decoration: none;
            word-break: break-all;
          ">${opts.inviteUrl}</a>
        </div>
      </td>
    </tr>
  `;

  return resend.emails.send({
    from: fromInvites,
    to: opts.to,
    subject: `${inviterRaw ? inviterName : "Invite"} • BlockPools`,
    html: renderBrandedEmail({
      title: "You're invited",
      preheader: `${inviterRaw ? inviterRaw : "A friend"} invited you to BlockPools.`,
      intro: "", // keep empty; bodyHtml controls layout
      bodyHtml,
      button: { label: "Accept invite", href: opts.inviteUrl },
      footerNote: "If you weren’t expecting this, you can ignore this email.",
    }),
  });
}

/**
 * Used by src/routes/emailTest.ts
 */
export async function sendTestEmail(opts: { to: string }) {
  const from = requireEnv("EMAIL_FROM_WELCOME");

  return resend.emails.send({
    from,
    to: opts.to,
    subject: "BlockPools email test",
    html: renderBrandedEmail({
      title: "Email delivery test",
      preheader: "Resend is working.",
      intro: "If you received this, Resend email delivery is working correctly.",
      footerNote: "You can ignore this message.",
    }),
  });
}
