package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/docker/docker/client"
)

type Server struct {
	cfg    Config
	docker *client.Client
	mux    *http.ServeMux
}

func NewServer(cfg Config) (*Server, error) {
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}

	s := &Server{cfg: cfg, docker: docker, mux: http.NewServeMux()}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /videos", s.listVideos)
	s.mux.HandleFunc("POST /videos", s.uploadVideo)
	s.mux.HandleFunc("GET /cameras", s.listCameras)
	s.mux.HandleFunc("POST /cameras", s.createCamera)
	s.mux.HandleFunc("DELETE /cameras/{camId}", s.deleteCamera)
	s.mux.HandleFunc("GET /health", s.health)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s", r.Method, r.URL.Path)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}
