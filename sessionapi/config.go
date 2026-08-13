package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	PostgresDSN   string
	DetectorTypes []string
	HTTPAddr      string
	DefaultLimit  int
	MaxLimit      int
}

func LoadConfig() (Config, error) {
	defaultLimit := 50
	if v := os.Getenv("DEFAULT_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid DEFAULT_LIMIT %q: %w", v, err)
		}
		defaultLimit = n
	}

	maxLimit := 500
	if v := os.Getenv("MAX_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid MAX_LIMIT %q: %w", v, err)
		}
		maxLimit = n
	}

	host := getEnvDefault("POSTGRESQL_HOST", "postgres")
	port := getEnvDefault("POSTGRESQL_PORT", "5432")
	user := getEnvDefault("POSTGRESQL_USER", "vms")
	pass := getEnvDefault("POSTGRESQL_PASS", "vms")
	db := getEnvDefault("POSTGRESQL_DB", "vms")

	detectors := splitAndTrim(getEnvDefault("DETECTOR_TYPES", "human,car"))
	if len(detectors) == 0 {
		return Config{}, fmt.Errorf("DETECTOR_TYPES must list at least one detector")
	}

	return Config{
		PostgresDSN:   fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db),
		DetectorTypes: detectors,
		HTTPAddr:      getEnvDefault("HTTP_ADDR", ":8090"),
		DefaultLimit:  defaultLimit,
		MaxLimit:      maxLimit,
	}, nil
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
