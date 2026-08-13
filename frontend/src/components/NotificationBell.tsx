import { useState } from "react";
import { useNotifications } from "../context/NotificationsContext";

export default function NotificationBell() {
  const { events, totalUnread, connectionStatus, markAllSeen } = useNotifications();
  const [open, setOpen] = useState(false);

  return (
    <div className="notification-bell">
      <button
        className="bell-button"
        onClick={() => {
          setOpen((o) => !o);
          markAllSeen();
        }}
        title={`SSE: ${connectionStatus}`}
      >
        <span className={`status-dot status-${connectionStatus === "open" ? "live" : "error"}`} />
        Notifications
        {totalUnread > 0 && <span className="badge">{totalUnread}</span>}
      </button>
      {open && (
        <div className="bell-dropdown">
          {events.length === 0 && <div className="bell-empty">No events yet.</div>}
          {events.slice(0, 20).map((event, i) => (
            <div key={`${event.kind}:${event.session_id}:${i}`} className="bell-row">
              <span className="bell-kind">{event.kind}</span>
              <span>{event.camera_id}</span>
              <span className="bell-time">{new Date(event.start_time).toLocaleTimeString()}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
