import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import type { OccupancyEvent } from "../lib/occupancyEvents";

const SSE_BASE = import.meta.env.VITE_SESSION_SSE_BASE ?? "http://localhost:8095";

type OccupancyByCamera = Record<string, number>;

const OccupancyContext = createContext<OccupancyByCamera | null>(null);

export function OccupancyProvider({ children }: { children: ReactNode }) {
  const [occupancy, setOccupancy] = useState<OccupancyByCamera>({});

  useEffect(() => {
    const es = new EventSource(`${SSE_BASE}/occupancy`);

    es.addEventListener("occupancy", (e: MessageEvent) => {
      try {
        const event = JSON.parse(e.data) as OccupancyEvent;
        setOccupancy((prev) => ({ ...prev, [event.camera_id]: event.count }));
      } catch (err) {
        console.error("failed to parse occupancy event", err, e.data);
      }
    });

    return () => es.close();
  }, []);

  return <OccupancyContext.Provider value={occupancy}>{children}</OccupancyContext.Provider>;
}

export function useOccupancy(): OccupancyByCamera {
  const ctx = useContext(OccupancyContext);
  if (!ctx) throw new Error("useOccupancy must be used within an OccupancyProvider");
  return ctx;
}
