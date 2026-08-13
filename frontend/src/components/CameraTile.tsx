import { useEffect, useRef, useState } from "react";
import { connectWhep, type WhepSession } from "../lib/whep";
import type { CameraConfig } from "../config/cameras";

const WHEP_BASE = import.meta.env.VITE_MEDIAMTX_WHEP_BASE ?? "http://localhost:8889";
const RECONNECT_DELAY_MS = 3000;

type Status = "connecting" | "live" | "error";

export default function CameraTile({ camera }: { camera: CameraConfig }) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [status, setStatus] = useState<Status>("connecting");

  useEffect(() => {
    let cancelled = false;
    let session: WhepSession | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    // Bumped on every (re)connect attempt so a stale connection's
    // onconnectionstatechange (e.g. its own teardown firing "closed" after
    // a newer attempt has already started) can't act on state it no
    // longer owns.
    let generation = 0;

    function scheduleReconnect(myGeneration: number) {
      if (cancelled || myGeneration !== generation || reconnectTimer) return;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connect();
      }, RECONNECT_DELAY_MS);
    }

    async function connect() {
      if (cancelled || !videoRef.current) return;
      generation += 1;
      const myGeneration = generation;
      setStatus("connecting");
      try {
        const newSession = await connectWhep(
          `${WHEP_BASE}/${camera.id}/whep`,
          videoRef.current,
          (state) => {
            if (cancelled || myGeneration !== generation) return;
            if (state === "connected") {
              setStatus("live");
            } else if (state === "failed" || state === "disconnected" || state === "closed") {
              setStatus("error");
              scheduleReconnect(myGeneration);
            }
          },
        );
        if (cancelled || myGeneration !== generation) {
          newSession.close();
          return;
        }
        session = newSession;
      } catch (err) {
        if (!cancelled && myGeneration === generation) {
          console.error(`WHEP connect failed for ${camera.id}`, err);
          setStatus("error");
          scheduleReconnect(myGeneration);
        }
      }
    }

    connect();

    return () => {
      cancelled = true;
      generation += 1; // invalidate any in-flight attempt's callbacks
      if (reconnectTimer) clearTimeout(reconnectTimer);
      session?.close();
    };
  }, [camera.id]);

  return (
    <div className="camera-tile">
      <video ref={videoRef} autoPlay playsInline muted />
      <div className="camera-tile-label">
        <span>{camera.name}</span>
        <span className={`status-dot status-${status}`} title={status} />
      </div>
    </div>
  );
}
