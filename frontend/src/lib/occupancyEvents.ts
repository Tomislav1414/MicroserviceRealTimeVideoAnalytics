// Mirrors session-sse's occupancyEvent JSON shape (session-sse/main.go).
// Unlike SessionEvent, this is a live snapshot of the latest frame's
// detection count, not a confirmed/deduplicated incident.
export interface OccupancyEvent {
  camera_id: string;
  count: number;
  ts: string; // RFC3339
}
