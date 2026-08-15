// Hardcoded on purpose: dynamic camera registration would need the grabber's
// static CAMERAS env var / Kafka partition count to become runtime-mutable,
// which is out of scope. To add a camera: wire a new mock-cam-* service in
// mock-rtsp/docker-compose.yaml (gives it a mediamtx path), add it to the
// grabber's CAMERAS env var, then add an entry here.
export interface CameraConfig {
  id: string; // mediamtx path, e.g. "cam/cam1" — also the camera_id used throughout the pipeline
  name: string; // display name
}

export const CAMERAS: CameraConfig[] = [
  { id: "cam/cam1", name: "Cam 1 — Human detection" },
  { id: "cam/cam2", name: "Cam 2 — Car passing" },
  { id: "cam/cam3", name: "Cam 3 — Worker zone" },
  { id: "cam/cam4", name: "Cam 4 — Store aisle" },
];
