package main

import "os"

type Config struct {
	MediaMTXHost       string
	MediaMTXPublicHost string
	MediaMTXPort       string
	VideosPath         string
	VideosFrom         string
	DockerNetwork      string
	CamImage           string
	Port               string
}

func configFromEnv() Config {
	return Config{
		MediaMTXHost:       getEnv("MEDIAMTX_HOST", "mediamtx"),
		MediaMTXPublicHost: getEnv("MEDIAMTX_PUBLIC_HOST", "localhost"),
		MediaMTXPort:       getEnv("MEDIAMTX_PORT", "8554"),
		VideosPath:         getEnv("VIDEOS_PATH", "/videos"),
		VideosFrom:         getEnv("VIDEOS_FROM", "mock-cam-controller"),
		DockerNetwork:      getEnv("DOCKER_NETWORK", "vms-local"),
		CamImage:           getEnv("CAM_IMAGE", "mock-cam:latest"),
		Port:               getEnv("PORT", "3000"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
