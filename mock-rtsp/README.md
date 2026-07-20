# mockRTSP

Local dev tool that fakes IP cameras. Instead of connecting real cameras, you spin up Docker containers that loop an `.mp4` file and stream it as RTSP — exactly like a real camera would. Your frontend connects to the same URL format it would use for a real camera.

---

## Architecture

```
docker-compose
  mock-cam (ffmpeg) ──RTSP publish──►
                                      MediaMTX
  mock-cam-2 (ffmpeg) ─RTSP publish──►  :8554 RTSP
                                         :8889 WebRTC (WHEP)
  mock-cam-controller                    :8888 HLS
    REST API :3000 ── spawns Docker       :9997 API
    containers on demand
                    │
                    ▼
              Your frontend
```

- **MediaMTX** — RTSP/WebRTC/HLS relay server. All cameras publish here. Your frontend reads from here.
- **mock-cam** — a Docker container running ffmpeg that loops an `.mp4` and pushes it to MediaMTX as RTSP.
- **mock-cam-controller** — REST API you call to create/delete cameras on demand without touching Docker yourself.

---

## How to start

```bash
docker compose up --build
```

This starts MediaMTX + 2 pre-configured cameras + the controller API.

---

## Stream URLs your frontend uses

| Protocol | URL pattern | Latency | Use for |
|----------|-------------|---------|---------|
| WebRTC (WHEP) | `http://localhost:8889/cam/<id>/whep` | ~100ms | browser frontends |
| RTSP | `rtsp://localhost:8554/cam/<id>` | ~200ms | native players, VLC |
| HLS | `http://localhost:8888/cam/<id>` | 2-6s | fallback only |

**Use WebRTC for your browser frontend** — it is the only protocol that gives sub-second latency in a browser. MediaMTX implements the WHEP standard so any WHEP-compatible player works.

### Playing WebRTC in the browser

MediaMTX ships a built-in player page you can use to verify a stream is working:

```
http://localhost:8889/cam/1
```

For your own frontend, use a WHEP client library and point it at the WHEP endpoint:

```
http://localhost:8889/cam/<id>/whep
```

### Static cameras (always available after `docker compose up`)

`cam/1` and `cam/2` are started automatically and require no API calls:

- `http://localhost:8889/cam/1/whep` — streams `sample.mp4`
- `http://localhost:8889/cam/2/whep` — streams `sample2.mp4`

---

## Controller API — manage cameras at runtime

Base URL: `http://localhost:3000`

### Create a camera

```http
POST /cameras
Content-Type: application/json

{ "camId": "lobby", "videoFile": "sample.mp4" }
```

Response:
```json
{
  "camId": "lobby",
  "containerName": "mock-cam-lobby",
  "videoFile": "sample.mp4",
  "rtspUrl": "rtsp://localhost:8554/cam/lobby",
  "hlsUrl": "http://localhost:8888/cam/lobby"
}
```

WebRTC WHEP endpoint for the created camera: `http://localhost:8889/cam/lobby/whep`

### List running cameras

```http
GET /cameras
```

### Delete a camera

```http
DELETE /cameras/lobby
```

### Upload a video file

```http
POST /videos
Content-Type: multipart/form-data
field: file = <your .mp4>
```

### List available videos

```http
GET /videos
```

### Health check

```http
GET /health
```

---

## Typical frontend workflow

1. `GET /cameras` on app start to see what is already running.
2. `POST /cameras` with a `camId` and `videoFile` to add a fake camera slot.
3. Construct the WebRTC URL: `http://localhost:8889/cam/<camId>/whep` and pass it to your WHEP player.
4. `DELETE /cameras/{camId}` when done to clean up.
