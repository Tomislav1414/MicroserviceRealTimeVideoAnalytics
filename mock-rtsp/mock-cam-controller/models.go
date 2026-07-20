package main

type CreateCameraRequest struct {
	CamID     string `json:"camId"`
	VideoFile string `json:"videoFile"`
}

type CameraResponse struct {
	CamID         string `json:"camId"`
	ContainerName string `json:"containerName"`
	VideoFile     string `json:"videoFile,omitempty"`
	RTSPUrl       string `json:"rtspUrl"`
	HLSUrl        string `json:"hlsUrl,omitempty"`
	Status        string `json:"status,omitempty"`
}

type VideoFile struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
