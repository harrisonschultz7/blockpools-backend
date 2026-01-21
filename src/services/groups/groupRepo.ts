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

function lower(a: any) {
  return String(a || "").toLowerCase();
}

export async function getGroupBySlug(slug: string): Promise<GroupRow | null> {
  const { data, error } = await supabaseAdmin()
    .from("groups")
    .select("id, slug, name, bio, created_at")
    .eq("slug", slug)
    .limit(1)
    .maybeSingle();

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

  return (data ?? []).map((g: any) => ({
    id: String(g.id),
    slug: String(g.slug),
    name: String(g.name),
    bio: g.bio ?? null,
    created_at: g.created_at ?? null,
  }));
}

// Active intervals logic requires joined_at and left_at columns.
// We return ALL intervals (not just currently active) because “active intervals” means within [joined_at, left_at].
export async function getGroupMemberIntervals(groupId: string): Promise<GroupMemberIntervalRow[]> {
  const { data, error } = await supabaseAdmin()
    .from("group_members")
    .select("group_id, user_address, joined_at, left_at")
    .eq("group_id", groupId);

  if (error) throw error;

  return (data ?? [])
    .map((m: any) => ({
      group_id: String(m.group_id),
      user_address: lower(m.user_address),
      joined_at: String(m.joined_at),
      left_at: m.left_at ? String(m.left_at) : null,
    }))
    .filter((x) => x.user_address && x.joined_at);
}

// Bulk member counts for leaderboard
export async function getMemberCountsByGroupIds(groupIds: string[]): Promise<Record<string, number>> {
  if (!groupIds.length) return {};

  const { data, error } = await supabaseAdmin()
    .from("group_members")
    .select("group_id")
    .in("group_id", groupIds);

  if (error) throw error;

  const counts: Record<string, number> = {};
  for (const r of data ?? []) {
    const gid = String((r as any)?.group_id || "");
    if (!gid) continue;
    counts[gid] = (counts[gid] || 0) + 1;
  }
  return counts;
}
