// src/services/groups/groupRepo.ts
import { supabaseAdmin } from "./supabaseAdmin";

export type GroupRow = {
  id: string;
  slug: string;
  name: string;
  bio: string | null;
  created_at?: string | null;
};

export type GroupMemberIntervalRow = {
  group_id: string;
  user_address: string;
  joined_at: string;
  left_at: string | null;
};

function lower(a: unknown) {
  return String(a || "").toLowerCase();
}

type RawGroupRow = {
  id: string;
  slug: string;
  name: string;
  bio?: string | null;
  created_at?: string | null;
};

type RawMemberRow = {
  group_id: string;
  user_address: string;
  joined_at: string;
  left_at?: string | null;
};

export async function getGroupBySlug(slug: string): Promise<GroupRow | null> {
  const { data, error } = await supabaseAdmin()
    .from("groups")
    .select("id, slug, name, bio, created_at")
    .eq("slug", slug)
    .limit(1)
    .maybeSingle<RawGroupRow>();

  if (error) throw error;
  if (!data) return null;

  return {
    id: String(data.id),
    slug: String(data.slug),
    name: String(data.name),
    bio: data.bio ?? null,
    created_at: data.created_at ?? null,
  };
}

export async function listGroups(limit = 200): Promise<GroupRow[]> {
  const { data, error } = await supabaseAdmin()
    .from("groups")
    .select("id, slug, name, bio, created_at")
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) throw error;

  const rows = (data ?? []) as RawGroupRow[];

  return rows.map((g) => ({
    id: String(g.id),
    slug: String(g.slug),
    name: String(g.name),
    bio: g.bio ?? null,
    created_at: g.created_at ?? null,
  }));
}

// Returns ALL membership intervals for the group.
// (Filtering to active-only happens in the metrics layer using joined_at/left_at.)
export async function getGroupMemberIntervals(groupId: string): Promise<GroupMemberIntervalRow[]> {
  const { data, error } = await supabaseAdmin()
    .from("group_members")
    .select("group_id, user_address, joined_at, left_at")
    .eq("group_id", groupId);

  if (error) throw error;

  const rows = (data ?? []) as RawMemberRow[];

  return rows
    .map((m) => ({
      group_id: String(m.group_id),
      user_address: lower(m.user_address),
      joined_at: String(m.joined_at),
      left_at: m.left_at ? String(m.left_at) : null,
    }))
    .filter((x: GroupMemberIntervalRow) => Boolean(x.user_address) && Boolean(x.joined_at));
}

// Bulk member counts for leaderboard (counts all rows; if you only want active members,
// you can change this query to .is("left_at", null) later.)
export async function getMemberCountsByGroupIds(groupIds: string[]): Promise<Record<string, number>> {
  if (!groupIds.length) return {};

  const { data, error } = await supabaseAdmin()
    .from("group_members")
    .select("group_id")
    .in("group_id", groupIds);

  if (error) throw error;

  const counts: Record<string, number> = {};
  for (const r of (data ?? []) as Array<{ group_id?: string | null }>) {
    const gid = String(r?.group_id || "");
    if (!gid) continue;
    counts[gid] = (counts[gid] || 0) + 1;
  }

  return counts;
}
