import { useNotifications } from "../context/NotificationsContext";
import { isDangerZoneEvent } from "../lib/sessionEvents";

function describe(kind: string): string {
  switch (kind) {
    case "STARTED":
      return "Session started";
    case "ENDED":
      return "Session ended";
    case "DANGER_ZONE_ENTRY":
      return "Danger zone entry";
    case "DANGER_ZONE_EXIT":
      return "Danger zone exit";
    default:
      return kind;
  }
}

export default function NotificationToastStack() {
  const { toasts, dismissToast } = useNotifications();

  return (
    <div className="toast-stack">
      {toasts.map(({ id, event }) => (
        <div key={id} className={`toast ${isDangerZoneEvent(event) ? "toast-urgent" : ""}`}>
          <div className="toast-title">{describe(event.kind)}</div>
          <div className="toast-body">
            {event.camera_id} · {event.detector}
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
