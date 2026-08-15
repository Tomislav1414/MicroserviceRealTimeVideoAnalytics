import { useNotifications } from "../context/NotificationsContext";
import { isDangerZoneEvent } from "../lib/sessionEvents";

// "cam/cam1" -> "cam1": the toast reads as a short human alert line, not a
// dump of the raw pipeline identifier.
function shortCamera(cameraId: string): string {
  return cameraId.replace(/^cam\//, "");
}

export default function NotificationToastStack() {
  const { toasts, dismissToast } = useNotifications();

  return (
    <div className="toast-stack">
      {toasts.map(({ id, event }) => (
        <div key={id} className={`toast ${isDangerZoneEvent(event) ? "toast-urgent" : ""}`}>
          <div className="toast-title">Alert</div>
          <div className="toast-body">
            on {shortCamera(event.camera_id)} detected {event.detector}
            {event.zone_name ? ` · zone: ${event.zone_name}` : ""}
          </div>
          <button className="toast-dismiss" onClick={() => dismissToast(id)} aria-label="Dismiss">
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
