// src/subgraph/client.ts
import { ENV } from "../config/env";

type GqlError = { message?: string };
type GqlResponse<T> = { data?: T; errors?: GqlError[] };

export async function subgraphQuery<TData>(
  query: string,
  variables: Record<string, any>
): Promise<TData> {
  const url = ENV.SUBGRAPH_QUERY_URL;
  if (!url) {
    throw new Error("Subgraph not configured: missing SUBGRAPH_QUERY_URL");
  }

  const headers: Record<string, string> = {
    "content-type": "application/json",
  };

  if (ENV.SUBGRAPH_AUTH_HEADER) {
    headers["authorization"] = ENV.SUBGRAPH_AUTH_HEADER;
  }

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({ query, variables }),
  });

  const text = await res.text();
  let json: GqlResponse<TData>;
  try {
    json = JSON.parse(text) as GqlResponse<TData>;
  } catch {
    throw new Error(`Subgraph non-JSON response (${res.status}): ${text.slice(0, 200)}`);
  }

  if (!res.ok) {
    const msg = json.errors?.[0]?.message || `HTTP ${res.status}`;
    throw new Error(`Subgraph HTTP error: ${msg}`);
  }

  if (json.errors?.length) {
    throw new Error(`Subgraph GQL error: ${json.errors[0]?.message || "unknown"}`);
  }

  if (!json.data) {
    throw new Error("Subgraph returned no data");
  }

  return json.data;
}
