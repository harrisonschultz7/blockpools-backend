export async function sendWelcomeEmail(opts: { to: string; username: string }) {
  requireEnv("RESEND_API_KEY");
  const fromWelcome = requireEnv("EMAIL_FROM_WELCOME");
  const appBaseUrl = requireEnv("APP_BASE_URL").replace(/\/+$/, "");

  const username = escapeHtml(opts.username);
  const ctaUrl = `${appBaseUrl}/app`;

  return resend.emails.send({
    from: fromWelcome,
    to: opts.to,
    subject: `Welcome to BlockPools, ${opts.username}`,
    html: `
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#0b1020;">
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#0b1020;">
      <tr>
        <td align="center" style="padding:28px 14px;">
          <!-- Container -->
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:560px;">
            <tr>
              <td style="
                border-radius:18px;
                overflow:hidden;
                background: radial-gradient(900px circle at 20% 0%, rgba(250,204,21,0.18), rgba(11,16,32,0) 55%),
                            radial-gradient(700px circle at 100% 20%, rgba(230,215,181,0.14), rgba(11,16,32,0) 55%),
                            linear-gradient(180deg, #101a3a 0%, #0b1020 100%);
                border:1px solid rgba(230,215,181,0.18);
                box-shadow: 0 20px 60px rgba(0,0,0,0.55);
              ">
                <!-- Header -->
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="padding:18px 20px 10px 20px;">
                      <div style="
                        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                        font-size:14px;
                        letter-spacing:0.2px;
                        color: rgba(230,215,181,0.9);
                        display:flex;
                        align-items:center;
                        gap:10px;
                      ">
                        <span style="
                          display:inline-block;
                          width:10px;height:10px;
                          border-radius:999px;
                          background:#facc15;
                          box-shadow:0 0 18px rgba(250,204,21,0.35);
                        "></span>
                        <span style="font-weight:700;">BlockPools</span>
                      </div>
                    </td>
                  </tr>
                  <tr>
                    <td style="padding:0 20px 6px 20px;">
                      <div style="
                        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                        color:#ffffff;
                        font-size:22px;
                        line-height:1.25;
                        font-weight:800;
                      ">
                        Welcome to BlockPools
                      </div>
                    </td>
                  </tr>
                  <tr>
                    <td style="padding:0 20px 16px 20px;">
                      <div style="
                        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                        color: rgba(219,227,255,0.88);
                        font-size:14px;
                        line-height:1.55;
                      ">
                        Your profile is live as <span style="color:#facc15;font-weight:700;">${username}</span>.
                        You can now explore markets and start trading.
                      </div>
                    </td>
                  </tr>

                  <!-- CTA -->
                  <tr>
                    <td style="padding:0 20px 6px 20px;">
                      <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                        <tr>
                          <td bgcolor="#facc15" style="border-radius:12px;">
                            <a href="${ctaUrl}"
                               style="
                                 display:inline-block;
                                 padding:12px 16px;
                                 border-radius:12px;
                                 text-decoration:none;
                                 font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                                 font-size:14px;
                                 font-weight:800;
                                 color:#0b1020;
                                 border:1px solid rgba(250,204,21,0.9);
                               ">
                              Open the App
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>

                  <!-- Secondary text -->
                  <tr>
                    <td style="padding:10px 20px 18px 20px;">
                      <div style="
                        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                        color: rgba(230,215,181,0.85);
                        font-size:12px;
                        line-height:1.55;
                      ">
                        Tip: connect your wallet to start trading USDC markets instantly.
                      </div>

                      <div style="
                        margin-top:14px;
                        height:1px;
                        background: linear-gradient(90deg, rgba(15,23,42,0), rgba(250,204,21,0.55), rgba(15,23,42,0));
                      "></div>

                      <div style="
                        margin-top:12px;
                        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                        color: rgba(230,215,181,0.65);
                        font-size:11px;
                        line-height:1.55;
                      ">
                        If you didn’t create this account, you can ignore this email.
                        <br/>
                        © ${new Date().getFullYear()} BlockPools
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Plain URL fallback -->
            <tr>
              <td style="padding:12px 8px 0 8px;">
                <div style="
                  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
                  font-size:11px;
                  color: rgba(230,215,181,0.55);
                  line-height:1.5;
                  text-align:center;
                  word-break:break-all;
                ">
                  Having trouble with the button? Paste this link into your browser:<br/>
                  <span style="color: rgba(219,227,255,0.7);">${ctaUrl}</span>
                </div>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
    `,
  });
}
