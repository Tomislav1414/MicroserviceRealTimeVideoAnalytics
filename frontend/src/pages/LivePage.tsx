import { CAMERAS } from "../config/cameras";
import CameraTile from "../components/CameraTile";

export default function LivePage() {
  return (
    <div className="camera-grid">
      {CAMERAS.map((camera) => (
        <CameraTile key={camera.id} camera={camera} />
      ))}
    </div>
  );
}
