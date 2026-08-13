import { useEffect, useState } from "react";
import { fetchSessions, type Session } from "../lib/sessionapi";
import { CAMERAS } from "../config/cameras";
import { DETECTORS } from "../config/detectors";

const PAGE_SIZE = 50;

export default function HistoryPage() {
  const [detector, setDetector] = useState(DETECTORS[0]);
  const [cameraId, setCameraId] = useState("");
  const [sessions, setSessions] = useState<Session[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);

  async function load(reset: boolean, currentSessions: Session[]) {
    setLoading(true);
    setError(null);
    try {
      const before = reset ? undefined : currentSessions[currentSessions.length - 1]?.start_time;
      const page = await fetchSessions({
        detector,
        cameraId: cameraId || undefined,
        before,
        limit: PAGE_SIZE,
      });
      setSessions((prev) => (reset ? page : [...prev, ...page]));
      setHasMore(page.length === PAGE_SIZE);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load(true, []);
  }, [detector, cameraId]);

  return (
    <div className="history-page">
      <div className="history-filters">
        <label>
          Detector
          <select value={detector} onChange={(e) => setDetector(e.target.value)}>
            {DETECTORS.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <label>
          Camera
          <select value={cameraId} onChange={(e) => setCameraId(e.target.value)}>
            <option value="">All cameras</option>
            {CAMERAS.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
      </div>

      {error && <div className="history-error">{error}</div>}

      <table className="history-table">
        <thead>
          <tr>
            <th>Camera</th>
            <th>Start</th>
            <th>End</th>
            <th>Count</th>
            <th>Session ID</th>
          </tr>
        </thead>
        <tbody>
          {sessions.map((s) => (
            <tr key={s.session_id}>
              <td>{s.camera_id}</td>
              <td>{new Date(s.start_time).toLocaleString()}</td>
              <td>{s.end_time ? new Date(s.end_time).toLocaleString() : "—"}</td>
              <td>{s.count}</td>
              <td className="mono">{s.session_id}</td>
            </tr>
          ))}
          {sessions.length === 0 && !loading && (
            <tr>
              <td colSpan={5} className="history-empty">
                No sessions found.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {hasMore && (
        <button disabled={loading} onClick={() => load(false, sessions)}>
          {loading ? "Loading…" : "Load more"}
        </button>
      )}
    </div>
  );
}
