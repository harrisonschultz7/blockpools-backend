// src/services/v2/v2MarketResolver.ts
//
// Reverse map: on-chain v2 marketId  ->  the stats shape the leaderboard/profile
// already understand (public.user_trade_events + public.games).
//
// v2 has NO per-game contract; every market shares one Exchange and is keyed by
// a keccak256 marketId. games.json (the same file the frontend + AMM pipeline
// use) is the source of truth for what each marketId means. We flatten it into
// a marketId -> entry map so a settled fill can be recorded with the exact same
// (gameId, outcomeIndex, outcomeCode, marketType) key an AMM trade would use.
//
// The one non-obvious mapping is MULTI-OUTCOME groups (3-way / league-winner):
// each is a GROUP of independent binary Yes/No sub-markets, one marketId per
// outcome. We record each sub-market as its own PROP (Yes = outcome 0, No =
// outcome 1) — structurally that is exactly what it is ("Will Arsenal win?
// Yes/No") — so a "No" bet is a first-class bet the leaderboard scores natively,
// with zero re-wiring. Parent linkage (parentGameId / parentOutcomeIndex /
// parentCode) is retained so a later profile task can group a game's
// sub-markets back into one visual position.

import fs from "fs";
import path from "path";
import bundledGamesJson from "../../data/games.json";

const GAMES_JSON_PATH = (process.env.GAMES_JSON_PATH || "").trim();

// Re-read the frontend games.json at most this often (it changes only on deploy).
const RELOAD_MS = Number(process.env.V2_GAMES_RELOAD_MS || 60_000);

export type OutcomeLeg = {
  outcomeIndex: number; // canonical AMM outcome index for user_trade_events
  outcomeCode: string; // team code | "YES" | "NO"
  side: "A" | "B" | null; // legacy binary side (kept for old UI semantics)
};

export type V2MarketEntry = {
  marketId: string; // lowercased 0x id
  kind: "binary" | "prop" | "group-sub";
  gameId: string; // id recorded to user_trade_events (sub-market id for groups)
  parentGameId: string; // group parent game id (== gameId when not a group)
  parentOutcomeIndex: number | null; // index within the parent group
  parentCode: string | null; // team code the sub-market is about (groups only)
  marketType: "BINARY" | "PROP" | "MULTI"; // recorded market_type (group-sub -> PROP)
  league: string | null;
  outcomesCount: number;
  marketQuestion: string | null;
  teamAName: string | null;
  teamBName: string | null;
  outcomes: Record<number, OutcomeLeg>; // per binary outcome (0 and 1)
};

type RawGame = Record<string, any>;

let cache: Map<string, V2MarketEntry> | null = null;
let cacheAt = 0;
let cacheSource = "";

function loadRaw(): { raw: Record<string, RawGame[]>; source: string } {
  if (GAMES_JSON_PATH) {
    try {
      const abs = path.isAbsolute(GAMES_JSON_PATH)
        ? GAMES_JSON_PATH
        : path.resolve(process.cwd(), GAMES_JSON_PATH);
      const txt = fs.readFileSync(abs, "utf8");
      return { raw: JSON.parse(txt), source: abs };
    } catch (err: any) {
      console.warn(
        `[v2MarketResolver] GAMES_JSON_PATH=${GAMES_JSON_PATH} unreadable, ` +
          `falling back to bundled games.json: ${err?.message ?? err}`
      );
    }
  }
  return { raw: bundledGamesJson as unknown as Record<string, RawGame[]>, source: "bundled" };
}

function slugId(s: string): string {
  return String(s || "").trim();
}

/** Stable, unique id for one group sub-market. */
export function subMarketGameId(parentGameId: string, code: string): string {
  return `${slugId(parentGameId)}::${String(code || "").toUpperCase()}`;
}

function isPropMarket(g: RawGame): boolean {
  return String(g?.marketType || "").toUpperCase() === "PROP";
}

function buildEntriesFor(g: RawGame, out: Map<string, V2MarketEntry>) {
  if (!g?.v2) return;
  const league = g.league != null ? String(g.league) : null;
  const parentGameId = String(g.gameId || "");

  // ---- MULTI-OUTCOME GROUP (3-way / league-winner) --------------------------
  // outcomes[] = independent Yes/No sub-markets, each its own marketId.
  if (Array.isArray(g.outcomes) && g.outcomes.length >= 2) {
    for (const o of g.outcomes) {
      const marketId = String(o?.marketId || "").toLowerCase();
      if (!marketId) continue;
      const code = String(o?.code || "").toUpperCase();
      const label = String(o?.label || o?.code || code);
      const parentIdx = Number.isFinite(Number(o?.outcomeIndex))
        ? Math.trunc(Number(o.outcomeIndex))
        : null;
      out.set(marketId, {
        marketId,
        kind: "group-sub",
        gameId: subMarketGameId(parentGameId, code),
        parentGameId,
        parentOutcomeIndex: parentIdx,
        parentCode: code,
        marketType: "PROP", // each sub-market scores like a Yes/No prop
        league,
        outcomesCount: 2,
        marketQuestion: `Will ${label} win?`,
        teamAName: "Yes",
        teamBName: "No",
        outcomes: {
          0: { outcomeIndex: 0, outcomeCode: "YES", side: "A" },
          1: { outcomeIndex: 1, outcomeCode: "NO", side: "B" },
        },
      });
    }
    return;
  }

  const marketId = String(g.marketId || "").toLowerCase();
  if (!marketId) return;

  // ---- SINGLE PROP (Yes/No) -------------------------------------------------
  if (isPropMarket(g)) {
    out.set(marketId, {
      marketId,
      kind: "prop",
      gameId: parentGameId,
      parentGameId,
      parentOutcomeIndex: null,
      parentCode: null,
      marketType: "PROP",
      league,
      outcomesCount: 2,
      marketQuestion: g.marketQuestion ? String(g.marketQuestion) : null,
      teamAName: g.teamAName ? String(g.teamAName) : "Yes",
      teamBName: g.teamBName ? String(g.teamBName) : "No",
      outcomes: {
        0: { outcomeIndex: 0, outcomeCode: "YES", side: "A" },
        1: { outcomeIndex: 1, outcomeCode: "NO", side: "B" },
      },
    });
    return;
  }

  // ---- BINARY MONEYLINE MATCHUP --------------------------------------------
  const codeA = String(g.teamACode || g.teamA || "A").toUpperCase();
  const codeB = String(g.teamBCode || g.teamB || "B").toUpperCase();
  out.set(marketId, {
    marketId,
    kind: "binary",
    gameId: parentGameId,
    parentGameId,
    parentOutcomeIndex: null,
    parentCode: null,
    marketType: "BINARY",
    league,
    outcomesCount: 2,
    marketQuestion: null,
    teamAName: g.teamAName ? String(g.teamAName) : codeA,
    teamBName: g.teamBName ? String(g.teamBName) : codeB,
    outcomes: {
      0: { outcomeIndex: 0, outcomeCode: codeA, side: "A" },
      1: { outcomeIndex: 1, outcomeCode: codeB, side: "B" },
    },
  });
}

function build(): Map<string, V2MarketEntry> {
  const { raw, source } = loadRaw();
  const out = new Map<string, V2MarketEntry>();
  for (const league of Object.keys(raw || {})) {
    const list = (raw as any)[league];
    if (!Array.isArray(list)) continue;
    for (const g of list) buildEntriesFor(g, out);
  }
  cacheSource = source;
  return out;
}

function getMap(): Map<string, V2MarketEntry> {
  const now = Date.now();
  if (!cache || now - cacheAt > RELOAD_MS) {
    cache = build();
    cacheAt = now;
  }
  return cache;
}

/** Force a reload on next access (call after a deploy writes new markets). */
export function invalidateV2Markets(): void {
  cache = null;
  cacheAt = 0;
}

/** Full entry for a marketId, or null if unknown. */
export function resolveV2Market(marketId: string): V2MarketEntry | null {
  if (!marketId) return null;
  return getMap().get(String(marketId).toLowerCase()) || null;
}

/** The specific outcome leg (0/1) plus its parent entry. */
export function resolveV2Leg(
  marketId: string,
  outcome: number
): { entry: V2MarketEntry; leg: OutcomeLeg } | null {
  const entry = resolveV2Market(marketId);
  if (!entry) return null;
  const leg = entry.outcomes[Number(outcome)];
  if (!leg) return null;
  return { entry, leg };
}

export function v2MarketsSource(): string {
  getMap();
  return cacheSource;
}
