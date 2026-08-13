package main

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers []string
	GroupID      string
	InputTopic   string
	OutputTopic  string
	JPEGQuality  int
	CommitEvery  int
}

func LoadConfig() (Config, error) {
	quality := 85
	if v := os.Getenv("JPEG_QUALITY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, err
		}
		quality = n
	}

	commitEvery := 25
	if v := os.Getenv("COMMIT_EVERY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, err
		}
		commitEvery = n
	}

	return Config{
		KafkaBrokers: splitAndTrim(getEnvDefault("KAFKA_BROKERS", "kafka:9092")),
		GroupID:      getEnvDefault("KAFKA_GROUP_ID", "decoder"),
		InputTopic:   getEnvDefault("INPUT_TOPIC", "raw-frames"),
		OutputTopic:  getEnvDefault("OUTPUT_TOPIC", "decoded-frames"),
		JPEGQuality:  quality,
		CommitEvery:  commitEvery,
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
