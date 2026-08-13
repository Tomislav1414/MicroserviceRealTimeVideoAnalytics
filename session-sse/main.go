package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type sessionEvent struct {
	Kind      string     `json:"kind"`
	SessionID string     `json:"session_id"`
	CameraID  string     `json:"camera_id"`
	Detector  string     `json:"detector"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
	Count     int64      `json:"count,omitempty"`
	ZoneID    *string    `json:"zone_id,omitempty"`
	ZoneName  *string    `json:"zone_name,omitempty"`
}

// closed reports whether this event represents a terminal state for its
// session (safe to evict from the history cache under memory pressure), as
// opposed to an open/live one that must be kept until its matching end event.
func (e sessionEvent) closed() bool {
	return e.Kind == "ENDED" || e.Kind == "DANGER_ZONE_EXIT"
}

// hub fans session events to SSE clients and maintains a bounded session
// cache for history replay on new connections.
type hub struct {
	mu       sync.RWMutex
	clients  map[*client]struct{}
	cache    map[string]sessionEvent // session_id -> latest event
	maxCache int
}

type client struct {
	camera string // "" = all cameras
	ch     chan []byte
}

func newHub(maxCache int) *hub {
	return &hub{
		clients:  make(map[*client]struct{}),
		cache:    make(map[string]sessionEvent),
		maxCache: maxCache,
	}
}

// ingest records the event in the cache (a later event for the same
// session_id, e.g. ENDED, overwrites an earlier one, e.g. STARTED) and
// broadcasts it to connected clients.
func (h *hub) ingest(e sessionEvent, log *slog.Logger) {
	frame := sseFrame(e)
	h.mu.Lock()
	h.cache[e.SessionID] = e
	if h.maxCache > 0 && len(h.cache) > h.maxCache {
		h.evictOldestClosedLocked(log)
	}
	clients := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	for _, c := range clients {
		if c.camera != "" && c.camera != e.CameraID {
			continue
		}
		select {
		case c.ch <- frame:
		default: // slow client; drop frame
		}
	}
}

// evictOldestClosedLocked drops the oldest closed (ENDED/DANGER_ZONE_EXIT)
// session to bound cache memory. Open sessions are never evicted so a
// client can't miss an in-progress session's eventual end. Caller must hold
// h.mu. If every cached entry is still open, the cache is left to grow past
// maxCache rather than dropping live state.
func (h *hub) evictOldestClosedLocked(log *slog.Logger) {
	var oldestID string
	var oldestStart time.Time
	for id, e := range h.cache {
		if !e.closed() {
			continue
		}
		if oldestID == "" || e.StartTime.Before(oldestStart) {
			oldestID, oldestStart = id, e.StartTime
		}
	}
	if oldestID == "" {
		return
	}
	delete(h.cache, oldestID)
	log.Debug("evicted session from cache", "session_id", oldestID, "cache_size", len(h.cache))
}

// snapshot returns all cached events matching the camera filter, sorted by
// start_time ascending so the client builds state in order.
func (h *hub) snapshot(camera string) []sessionEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]sessionEvent, 0, len(h.cache))
	for _, e := range h.cache {
		if camera == "" || e.CameraID == camera {
			out = append(out, e)
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].StartTime.Before(out[j-1].StartTime); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (h *hub) add(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	maxCache, err := strconv.Atoi(env("MAX_CACHE_SESSIONS", "5000"))
	if err != nil || maxCache < 0 {
		log.Error("invalid MAX_CACHE_SESSIONS, must be a non-negative integer", "value", os.Getenv("MAX_CACHE_SESSIONS"))
		os.Exit(1)
	}
	h := newHub(maxCache)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", h.serveSSE)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	addr := env("HTTP_ADDR", ":8095")
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Info("sse server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			stop()
		}
	}()
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scancel()
		_ = srv.Shutdown(sctx)
	}()

	topic := env("SESSIONS_TOPIC", "sessions")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ","),
		Topic:   topic,
		// No GroupID: this service is a singleton in-memory fan-out hub, not
		// a horizontally-scaled consumer — running two replicas wouldn't
		// share a client list anyway. A plain partition reader with
		// StartOffset FirstOffset guarantees the full topic replays into
		// the cache on every start. A consumer group would NOT do this
		// reliably: kafka-go auto-commits offsets by default, so
		// StartOffset only applies once (before the group has any
		// committed offset) and every later restart would resume
		// mid-stream instead of rebuilding history.
		Partition:   0, // the sessions topic is created with a single partition (see risingwave/docker-compose.yaml kafka-init)
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	log.Info("session-sse consuming", "topic", topic, "max_cache_sessions", maxCache)

	const readErrBackoff = 2 * time.Second
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("shutting down")
				return
			}
			log.Error("read", "err", err)
			select {
			case <-ctx.Done():
				log.Info("shutting down")
				return
			case <-time.After(readErrBackoff):
			}
			continue
		}
		var e sessionEvent
		if err := json.Unmarshal(m.Value, &e); err != nil {
			log.Warn("skip malformed", "err", err)
			continue
		}
		h.ingest(e, log)
	}
}

// sseFrame encodes one event as an SSE frame. The event name lets clients
// subscribe with addEventListener instead of branching on the "kind" field.
func sseFrame(e sessionEvent) []byte {
	b, _ := json.Marshal(e)
	var event string
	switch e.Kind {
	case "STARTED":
		event = "started"
	case "ENDED":
		event = "ended"
	case "DANGER_ZONE_ENTRY":
		event = "danger_zone_entry"
	case "DANGER_ZONE_EXIT":
		event = "danger_zone_exit"
	default:
		event = "message"
	}
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", event, b)
}

func (h *hub) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	camera := r.URL.Query().Get("camera_id")
	c := &client{camera: camera, ch: make(chan []byte, 64)}

	// Send history before registering for live events so no event is missed
	// in the gap between the snapshot and the subscription.
	for _, e := range h.snapshot(camera) {
		_, _ = w.Write(sseFrame(e))
	}
	flusher.Flush()

	h.add(c)
	defer h.remove(c)

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case frame := <-c.ch:
			_, _ = w.Write(frame)
			flusher.Flush()
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
