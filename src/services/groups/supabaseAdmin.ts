// src/services/groups/supabaseAdmin.ts
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let _client: SupabaseClient | null = null;

export function supabaseAdmin(): SupabaseClient {
  if (_client) return _client;

  const url = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;

  // Prefer service role key on backend, but allow anon key fallback for your current RLS policies.
  const key =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_ANON_KEY ||
    process.env.VITE_SUPABASE_ANON_KEY;

  if (!url) {
    throw new Error("Missing SUPABASE_URL (or VITE_SUPABASE_URL) env var on backend.");
  }

  if (!key) {
    throw new Error(
      "Missing Supabase key env var. Set SUPABASE_SERVICE_ROLE_KEY (preferred) or SUPABASE_ANON_KEY / VITE_SUPABASE_ANON_KEY."
    );
  }

  _client = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  return _client;
}
