// src/services/groups/supabaseAdmin.ts
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let _client: SupabaseClient | null = null;

export function supabaseAdmin(): SupabaseClient {
  if (_client) return _client;

  const url = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url) throw new Error("Missing SUPABASE_URL (or VITE_SUPABASE_URL) env var on backend.");
  if (!key) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY env var on backend.");

  _client = createClient(url, key, {
    auth: { persistSession: false },
  });

  return _client;
}
