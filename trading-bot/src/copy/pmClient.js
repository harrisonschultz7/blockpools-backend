// trading-bot/src/copy/pmClient.js
//
// A Polymarket trading client for SOMEONE ELSE'S wallet.
//
// The frontend builds its client from a viem wallet client over the browser's
// EIP-1193 provider, which pops Privy's signer. Server side there is no browser
// and no prompt: the subscriber granted delegation once, and Privy will sign on
// our behalf through walletApi. So the shape is identical and only the transport
// differs -- a provider whose `request` routes signing to Privy instead of to a
// wallet extension.
//
// THE SESSION SIGNER IS THE WHOLE SECURITY BOUNDARY. This app's wallets live in
// Privy's TEE, so access is granted by the user adding OUR key quorum as a
// session signer on their wallet. Two things must both hold for a signature:
// the user added the quorum, and we can prove we hold that quorum's private key
// (PRIVY_AUTHORIZATION_PRIVATE_KEY). Revoking either one stops us dead. This
// module does not check the grant itself -- it cannot be trusted to -- it
// simply asks, and a revoked wallet fails at the signature.
//
// Builder attribution goes through the SAME /pm/sign endpoint the frontend uses
// rather than putting the builder secret on this box. One copy of a secret is
// easier to rotate and impossible to leak from two places.

const { PrivyClient } = require("@privy-io/server-auth");
const log = require("../log");

const PRIVY_APP_ID = (process.env.PRIVY_APP_ID || "").trim();
const PRIVY_APP_SECRET = (process.env.PRIVY_APP_SECRET || "").trim();
/** Absolute URL of the deployed /pm/sign function. */
const PM_SIGNING_URL = (process.env.PM_SIGNING_URL || "").trim();
/**
 * Private key of the authorization keypair (key quorum) registered in the Privy
 * dashboard. Wallet RPC is rejected without it once a quorum exists, so this is
 * not optional -- failing at construction beats failing per-signature, which
 * would look like a Polymarket problem.
 */
const PRIVY_AUTHORIZATION_PRIVATE_KEY =
  (process.env.PRIVY_AUTHORIZATION_PRIVATE_KEY || "").trim();

let privy = null;
function privyClient() {
  if (!privy) {
    if (!PRIVY_APP_ID || !PRIVY_APP_SECRET) {
      throw new Error("PRIVY_APP_ID / PRIVY_APP_SECRET missing");
    }
    if (!PRIVY_AUTHORIZATION_PRIVATE_KEY) {
      throw new Error("PRIVY_AUTHORIZATION_PRIVATE_KEY missing");
    }
    privy = new PrivyClient(PRIVY_APP_ID, PRIVY_APP_SECRET, {
      walletApi: { authorizationPrivateKey: PRIVY_AUTHORIZATION_PRIVATE_KEY },
    });
  }
  return privy;
}

/**
 * An EIP-1193 provider backed by a delegated Privy wallet.
 *
 * Only the methods the Polymarket SDK actually calls are implemented, and
 * anything else throws by name rather than returning undefined -- a provider
 * that silently answers a method it does not support produces a signature
 * failure three layers away from the cause.
 */
function privyProvider(address) {
  return {
    async request({ method, params }) {
      const api = privyClient().walletApi;

      if (method === "eth_accounts" || method === "eth_requestAccounts") {
        return [address];
      }
      if (method === "eth_chainId") return "0x89"; // Polygon

      if (method === "eth_signTypedData_v4") {
        // params: [address, typedDataJsonOrObject]
        const raw = params[1];
        const typedData = typeof raw === "string" ? JSON.parse(raw) : raw;
        const { signature } = await api.ethereum.signTypedData({
          address,
          chainType: "ethereum",
          typedData,
        });
        return signature;
      }

      if (method === "personal_sign") {
        // params: [messageHex, address]
        const { signature } = await api.ethereum.signMessage({
          address,
          chainType: "ethereum",
          message: params[0],
        });
        return signature;
      }

      throw new Error(`privyProvider: unsupported method ${method}`);
    },
  };
}

/**
 * One cached client per subscriber wallet.
 *
 * createSecureClient is expensive -- it derives the Deposit Wallet, deploys it
 * gaslessly if missing, and negotiates CLOB credentials -- so it must not run
 * once per order. Cached by EOA the way the frontend caches per signer, and
 * dropped on failure so a transient error does not poison the wallet for the
 * lifetime of the process.
 */
const clients = new Map();

async function getPmClient(eoaAddress) {
  if (!PM_SIGNING_URL) {
    throw new Error("PM_SIGNING_URL missing -- orders would be unattributed");
  }
  const key = String(eoaAddress).toLowerCase();
  const cached = clients.get(key);
  if (cached) return cached;

  const promise = (async () => {
    const [{ createSecureClient, remoteBuilderSigning }, { signerFrom }, viem, chains] =
      await Promise.all([
        import("@polymarket/client"),
        import("@polymarket/client/viem"),
        import("viem"),
        import("viem/chains"),
      ]);

    const walletClient = viem.createWalletClient({
      account: eoaAddress,
      chain: chains.polygon,
      transport: viem.custom(privyProvider(eoaAddress)),
    });

    const client = await createSecureClient({
      signer: signerFrom(walletClient),
      apiKey: remoteBuilderSigning({ url: PM_SIGNING_URL }),
    });

    // Without approvals the first order fails at the exchange rather than here,
    // which reads as a rejected trade instead of an unconfigured wallet.
    const approvals = await client.fetchTradingApprovalsState();
    if (!approvals.isFullyApproved) {
      log(`  copy: setting trading approvals for ${eoaAddress.slice(0, 10)}…`);
      await client.setupTradingApprovals();
    }
    return client;
  })();

  clients.set(key, promise);
  promise.catch(() => clients.delete(key));
  return promise;
}

/** Drop a cached client, e.g. after a delegation revoke. */
function forgetPmClient(eoaAddress) {
  clients.delete(String(eoaAddress).toLowerCase());
}

module.exports = { getPmClient, forgetPmClient, privyProvider };
