const API_BASE = import.meta.env.VITE_SESSION_API_BASE ?? "http://localhost:8090";

export interface Session {
  session_id: string;
  camera_id: string;
  start_time: string;
  end_time?: string;
  count: number;
}

export async function fetchSessions(params: {
  detector: string;
  cameraId?: string;
  before?: string;
  limit?: number;
}): Promise<Session[]> {
  const q = new URLSearchParams({ detector: params.detector });
  if (params.cameraId) q.set("camera_id", params.cameraId);
  if (params.before) q.set("before", params.before);
  if (params.limit) q.set("limit", String(params.limit));

  const resp = await fetch(`${API_BASE}/sessions?${q.toString()}`);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}) as { error?: string });
    throw new Error(body.error ?? `sessionapi request failed: ${resp.status}`);
  }
  return resp.json();
}
