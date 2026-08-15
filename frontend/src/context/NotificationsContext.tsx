import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { isDangerZoneEvent, isOpenEvent, type SessionEvent } from "../lib/sessionEvents";

const SSE_BASE = import.meta.env.VITE_SESSION_SSE_BASE ?? "http://localhost:8095";
const MAX_EVENTS = 200;
const TOAST_DURATION_MS = 6000;
const DANGER_ZONE_TOAST_DURATION_MS = 12000;

export interface Toast {
  id: string;
  event: SessionEvent;
}

interface NotificationsValue {
  events: SessionEvent[];
  unreadByCamera: Record<string, number>;
  totalUnread: number;
  toasts: Toast[];
  connectionStatus: "connecting" | "open" | "error";
  markAllSeen: () => void;
  dismissToast: (id: string) => void;
}

const NotificationsContext = createContext<NotificationsValue | null>(null);

export function NotificationsProvider({ children }: { children: ReactNode }) {
  const [events, setEvents] = useState<SessionEvent[]>([]);
  const [unreadByCamera, setUnreadByCamera] = useState<Record<string, number>>({});
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<"connecting" | "open" | "error">(
    "connecting",
  );

  const dismissToast = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const handleIncomingEvent = useCallback(
    (event: SessionEvent) => {
      setEvents((prev) => [event, ...prev].slice(0, MAX_EVENTS));
      setUnreadByCamera((prev) => ({
        ...prev,
        [event.camera_id]: (prev[event.camera_id] ?? 0) + 1,
      }));

      // Only "opening" events (STARTED / DANGER_ZONE_ENTRY) get a toast --
      // ENDED/DANGER_ZONE_EXIT still land in `events`/the unread badge, just
      // without popping up, per explicit request to surface only the start
      // of an incident, not its close.
      if (!isOpenEvent(event)) return;

      const toastId = `${event.kind}:${event.session_id}`;
      setToasts((prev) => (prev.some((t) => t.id === toastId) ? prev : [...prev, { id: toastId, event }]));
      setTimeout(
        () => dismissToast(toastId),
        isDangerZoneEvent(event) ? DANGER_ZONE_TOAST_DURATION_MS : TOAST_DURATION_MS,
      );
    },
    [dismissToast],
  );

  useEffect(() => {
    // No camera_id filter: this connection must see every camera's events
    // regardless of what's currently on screen, since that's the entire
    // point of a global notification feed.
    const es = new EventSource(`${SSE_BASE}/events`);

    const onMessage = (e: MessageEvent) => {
      try {
        handleIncomingEvent(JSON.parse(e.data) as SessionEvent);
      } catch (err) {
        console.error("failed to parse session event", err, e.data);
      }
    };

    es.addEventListener("started", onMessage);
    es.addEventListener("ended", onMessage);
    es.addEventListener("danger_zone_entry", onMessage);
    es.addEventListener("danger_zone_exit", onMessage);
    es.addEventListener("open", () => setConnectionStatus("open"));
    // EventSource retries on its own after an error; this just reflects
    // that in the UI, no manual reconnect logic needed here.
    es.addEventListener("error", () => setConnectionStatus("error"));

    return () => es.close();
  }, [handleIncomingEvent]);

  const totalUnread = useMemo(
    () => Object.values(unreadByCamera).reduce((sum, n) => sum + n, 0),
    [unreadByCamera],
  );

  const markAllSeen = useCallback(() => setUnreadByCamera({}), []);

  const value = useMemo<NotificationsValue>(
    () => ({
      events,
      unreadByCamera,
      totalUnread,
      toasts,
      connectionStatus,
      markAllSeen,
      dismissToast,
    }),
    [events, unreadByCamera, totalUnread, toasts, connectionStatus, markAllSeen, dismissToast],
  );

  return <NotificationsContext.Provider value={value}>{children}</NotificationsContext.Provider>;
}

export function useNotifications(): NotificationsValue {
  const ctx = useContext(NotificationsContext);
  if (!ctx) throw new Error("useNotifications must be used within a NotificationsProvider");
  return ctx;
}
