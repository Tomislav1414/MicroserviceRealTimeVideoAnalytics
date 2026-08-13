// Hardcoded on purpose: dynamic camera registration would need the grabber's
// static CAMERAS env var / Kafka partition count to become runtime-mutable,
// which is out of scope. To add a camera: wire a new mock-cam-* service in
// mock-rtsp/docker-compose.yaml (gives it a mediamtx path), add it to the
// grabber's CAMERAS env var, then add an entry here.
export interface CameraConfig {
  id: string; // mediamtx path, e.g. "cam/hum_det" — also the camera_id used throughout the pipeline
  name: string; // display name
}

export const CAMERAS: CameraConfig[] = [
  { id: "cam/hum_det", name: "Human detection cam" },
  { id: "cam/car_passing", name: "Car passing cam" },
];
