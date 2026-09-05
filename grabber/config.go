package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Cameras         []string
	MediaMTXHost    string
	MediaMTXPort    string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaPartitions int
}

func LoadConfig() (Config, error) {
	camerasEnv := os.Getenv("CAMERAS")
	if camerasEnv == "" {
		return Config{}, fmt.Errorf("CAMERAS env var is required (comma-separated camera IDs, e.g. cam/hum_det,cam/car_passing)")
	}
	cameras := splitAndTrim(camerasEnv)

	brokers := splitAndTrim(getEnvDefault("KAFKA_BROKERS", "kafka:9092"))

	partitions := len(cameras)
	if v := os.Getenv("KAFKA_PARTITIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid KAFKA_PARTITIONS %q: %w", v, err)
		}
		partitions = n
	}

	if partitions < len(cameras) {
		return Config{}, fmt.Errorf("KAFKA_PARTITIONS (%d) must be >= number of cameras (%d)", partitions, len(cameras))
	}

	return Config{
		Cameras:         cameras,
		MediaMTXHost:    getEnvDefault("MEDIAMTX_HOST", "mediamtx"),
		MediaMTXPort:    getEnvDefault("MEDIAMTX_PORT", "8554"),
		KafkaBrokers:    brokers,
		KafkaTopic:      getEnvDefault("KAFKA_TOPIC", "raw-frames"),
		KafkaPartitions: partitions,
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
