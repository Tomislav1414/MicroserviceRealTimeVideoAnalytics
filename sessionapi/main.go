package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Session struct {
	SessionID string     `json:"session_id"`
	CameraID  string     `json:"camera_id"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
	Count     int64      `json:"count"`
}

type api struct {
	db        *sql.DB
	log       *slog.Logger
	cfg       Config
	detectors map[string]struct{} 
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := LoadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Error("db open", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	detectors := make(map[string]struct{}, len(cfg.DetectorTypes))
	for _, d := range cfg.DetectorTypes {
		detectors[d] = struct{}{}
	}
	a := &api{db: db, log: log, cfg: cfg, detectors: detectors}

	mux := http.NewServeMux()
	mux.HandleFunc("/sessions", a.handleSessions)
	mux.HandleFunc("/healthz", a.handleHealthz)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: withCORS(mux)}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("sessionapi listening", "addr", cfg.HTTPAddr, "detectors", cfg.DetectorTypes)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func (a *api) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.db.PingContext(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"db unreachable"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (a *api) handleSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	detector := q.Get("detector")
	if _, ok := a.detectors[detector]; !ok {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("detector must be one of %v", a.cfg.DetectorTypes))
		return
	}

	limit := a.cfg.DefaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			httpError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > a.cfg.MaxLimit {
		limit = a.cfg.MaxLimit
	}

	var before *time.Time
	if v := q.Get("before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			httpError(w, http.StatusBadRequest, "before must be an RFC3339 timestamp")
			return
		}
		before = &t
	}

	cameraID := q.Get("camera_id")

	query := fmt.Sprintf(`
		SELECT session_id, camera_id, start_time, end_time, count
		FROM %s_detection_sessions_log
		WHERE ($1 = '' OR camera_id = $1)
		  AND ($2::timestamptz IS NULL OR start_time < $2)
		ORDER BY start_time DESC
		LIMIT $3`, detector)

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := a.db.QueryContext(ctx, query, cameraID, before, limit)
	if err != nil {
		a.log.Error("query", "err", err)
		httpError(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	sessions := make([]Session, 0, limit)
	for rows.Next() {
		var s Session
		if err := rows.Scan(&s.SessionID, &s.CameraID, &s.StartTime, &s.EndTime, &s.Count); err != nil {
			a.log.Error("scan", "err", err)
			httpError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		sessions = append(sessions, s)
	}
	if err := rows.Err(); err != nil {
		a.log.Error("rows", "err", err)
		httpError(w, http.StatusInternalServerError, "row iteration failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sessions)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
