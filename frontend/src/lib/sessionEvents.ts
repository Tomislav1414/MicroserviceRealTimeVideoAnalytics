// Mirrors session-sse's sessionEvent JSON shape (session-sse/main.go) and,
// transitively, the `sessions` Kafka topic payload produced by each
// detector's RisingWave pipeline (risingwave/pipeline.sql).
export type SessionEventKind =
  | "STARTED"
  | "ENDED"
  | "DANGER_ZONE_ENTRY"
  | "DANGER_ZONE_EXIT";

export interface SessionEvent {
  kind: SessionEventKind;
  session_id: string;
  camera_id: string;
  detector: string;
  start_time: string; // RFC3339
  end_time?: string; // RFC3339, present once ended
  count?: number;
  zone_id?: string;
  zone_name?: string;
}

export function isDangerZoneEvent(e: SessionEvent): boolean {
  return e.kind === "DANGER_ZONE_ENTRY" || e.kind === "DANGER_ZONE_EXIT";
}

export function isOpenEvent(e: SessionEvent): boolean {
  return e.kind === "STARTED" || e.kind === "DANGER_ZONE_ENTRY";
}
