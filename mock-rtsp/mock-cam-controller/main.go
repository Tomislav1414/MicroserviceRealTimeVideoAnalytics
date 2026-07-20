package main

import (
	"log"
	"net/http"
)

func main() {
	cfg := configFromEnv()

	srv, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	addr := ":" + cfg.Port
	log.Printf("[controller] listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
