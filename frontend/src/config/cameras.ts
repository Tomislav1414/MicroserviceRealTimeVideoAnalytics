// Hardcoded on purpose: dynamic camera registration would need the grabber's
// static CAMERAS env var / Kafka partition count to become runtime-mutable,
// which is out of scope. To add a camera: wire a new mock-cam-* service in
// mock-rtsp/docker-compose.yaml (gives it a mediamtx path), add it to the
// grabber's CAMERAS env var, then add an entry here.
export interface CameraConfig {
  id: string; // mediamtx path, e.g. "cam/cam1" — also the camera_id used throughout the pipeline
  name: string; // display name
}

// Layout 1: the original 4 cameras. Each detector's own CAMERAS env var
// (cardetector/humandetector/firedetector docker-compose.yaml) now scopes it
// to a subset of cameras rather than running every detector on every frame —
// cam1: human only, cam2: car only, cam3: no detection (stream only, though
// still decoded), cam4: no session detection of its own, only feeds
// session-sse's /occupancy live people-count (see OCCUPANCY_CAMERA_ID there).
export const CAMERAS: CameraConfig[] = [
  { id: "cam/cam1", name: "Cam 1 — Human detection" },
  { id: "cam/cam2", name: "Cam 2 — Car passing" },
  { id: "cam/cam3", name: "Cam 3 — Worker zone" },
  { id: "cam/cam4", name: "Cam 4 — Store aisle" },
];

// Layout 2: cam5/cam6 are stream-only (never added to grabber's CAMERAS, so
// they never enter the detection pipeline at all -- no grabber/decoder/Kafka
// involvement, just mediamtx -> WHEP). cam7/cam8 are decoded and scoped to
// the fire/smoke detector only (firedetector's CAMERAS env var).
export const LAYOUT2_CAMERAS: CameraConfig[] = [
  { id: "cam/cam5", name: "Cam 5 — FP30" },
  { id: "cam/cam6", name: "Cam 6 — FP15" },
  { id: "cam/cam7", name: "Cam 7 — Living room fire" },
  { id: "cam/cam8", name: "Cam 8 — Room fire" },
];

// History's camera filter isn't tied to one layout, so it needs every camera
// across both.
export const ALL_CAMERAS: CameraConfig[] = [...CAMERAS, ...LAYOUT2_CAMERAS];
