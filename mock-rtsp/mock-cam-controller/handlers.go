package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listVideos(w http.ResponseWriter, r *http.Request) {
	entries, err := os.ReadDir(s.cfg.VideosPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read videos directory")
		return
	}

	videos := []VideoFile{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".mp4") {
			videos = append(videos, VideoFile{
				Name: e.Name(),
				Path: filepath.Join(s.cfg.VideosPath, e.Name()),
			})
		}
	}

	writeJSON(w, http.StatusOK, videos)
}

// better way to handle large files?
func (s *Server) uploadVideo(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(500 << 20); err != nil { // 500 MB limit
		writeError(w, http.StatusBadRequest, "failed to parse form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file field is required")
		return
	}
	defer file.Close()

	if !strings.HasSuffix(header.Filename, ".mp4") {
		writeError(w, http.StatusBadRequest, "only .mp4 files are accepted")
		return
	}

	dest := filepath.Join(s.cfg.VideosPath, filepath.Base(header.Filename))
	out, err := os.Create(dest)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save file")
		return
	}
	defer out.Close()

	if _, err := io.Copy(out, file); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to write file")
		return
	}

	writeJSON(w, http.StatusCreated, VideoFile{Name: header.Filename, Path: dest})
}

func (s *Server) createCamera(w http.ResponseWriter, r *http.Request) {
	var req CreateCameraRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.CamID == "" || req.VideoFile == "" {
		writeError(w, http.StatusBadRequest, "camId and videoFile are required")
		return
	}

	videoPath := filepath.Join(s.cfg.VideosPath, req.VideoFile)
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("video file not found: %s", req.VideoFile))
		return
	}

	containerName := "mock-cam-" + req.CamID
	ctx := context.Background()

	// Pull image if not present locally
	if err := s.ensureImage(ctx, s.cfg.CamImage); err != nil {
		log.Printf("warn: could not pull image: %v", err)
	}

	resp, err := s.docker.ContainerCreate(ctx,
		&container.Config{
			Image: s.cfg.CamImage,
			Env: []string{
				fmt.Sprintf("CAM_ID=cam/%s", req.CamID),
				fmt.Sprintf("VIDEO_PATH=/videos/%s", req.VideoFile),
				fmt.Sprintf("RTSP_HOST=%s", s.cfg.MediaMTXHost),
				fmt.Sprintf("RTSP_PORT=%s", s.cfg.MediaMTXPort),
			},
		},
		&container.HostConfig{
			// Reuse /videos mount from controller container so dynamic camera containers
			// can access uploaded files without relying on host-specific absolute paths.
			VolumesFrom:   []string{s.cfg.VideosFrom + ":ro"},
			NetworkMode:   container.NetworkMode(s.cfg.DockerNetwork),
			RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
		},
		&network.NetworkingConfig{},
		nil,
		containerName,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create container: %v", err))
		return
	}

	if err := s.docker.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start container: %v", err))
		return
	}

	writeJSON(w, http.StatusCreated, CameraResponse{
		CamID:         req.CamID,
		ContainerName: containerName,
		VideoFile:     req.VideoFile,
		RTSPUrl:       fmt.Sprintf("rtsp://%s:%s/cam/%s", s.cfg.MediaMTXPublicHost, s.cfg.MediaMTXPort, req.CamID),
		//hardcoded HLS URL
		HLSUrl: fmt.Sprintf("http://localhost:8888/cam/%s", req.CamID),
	})
}

func (s *Server) listCameras(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	containers, err := s.docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("name", "mock-cam-")),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list containers")
		return
	}

	cameras := []CameraResponse{}
	for _, c := range containers {
		name := c.Names[0] // e.g. "/mock-cam-office-lobby"
		camID := strings.TrimPrefix(name, "/mock-cam-")
		cameras = append(cameras, CameraResponse{
			CamID:         camID,
			ContainerName: strings.TrimPrefix(name, "/"),
			Status:        c.Status,
			RTSPUrl: fmt.Sprintf("rtsp://%s:%s/cam/%s",
				s.cfg.MediaMTXPublicHost, s.cfg.MediaMTXPort, camID),
		})
	}

	writeJSON(w, http.StatusOK, cameras)
}

func (s *Server) deleteCamera(w http.ResponseWriter, r *http.Request) {
	camID := r.PathValue("camId")
	containerName := "mock-cam-" + camID
	ctx := context.Background()

	if err := s.docker.ContainerStop(ctx, containerName, container.StopOptions{}); err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("container not found or already stopped: %v", err))
		return
	}

	if err := s.docker.ContainerRemove(ctx, containerName, container.RemoveOptions{}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to remove container: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"deleted": camID})
}
