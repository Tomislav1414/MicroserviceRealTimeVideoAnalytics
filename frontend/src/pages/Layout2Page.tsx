import { LAYOUT2_CAMERAS } from "../config/cameras";
import CameraTile from "../components/CameraTile";

export default function Layout2Page() {
  return (
    <div className="camera-grid">
      {LAYOUT2_CAMERAS.map((camera) => (
        <CameraTile key={camera.id} camera={camera} />
      ))}
    </div>
  );
}
