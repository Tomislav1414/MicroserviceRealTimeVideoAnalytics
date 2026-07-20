# mock-cam-controller API

REST API that spins up/down mock RTSP camera containers and manages video files.

Base URL: `http://localhost:3000` (default, controlled by `PORT` env var)

---

## Endpoints

### GET /health

Health check.

**Response `200`**
```json
{ "status": "ok" }
```

---

### GET /videos

List all uploaded `.mp4` video files available for use as camera streams.

**Response `200`**
```json
[
  { "name": "parking-lot.mp4", "path": "/videos/parking-lot.mp4" },
  { "name": "lobby.mp4",       "path": "/videos/lobby.mp4" }
]
```

---

### POST /videos

Upload an `.mp4` file. Must be sent as `multipart/form-data` with the file under the field name `file`.

**Request**
```
Content-Type: multipart/form-data
field: file = <.mp4 binary>
```

**Response `201`**
```json
{ "name": "parking-lot.mp4", "path": "/videos/parking-lot.mp4" }
```

**Errors**
| Status | Reason |
|--------|--------|
| `400`  | Not a `.mp4` file, or `file` field missing |
| `500`  | Failed to save file on server |

---

### GET /cameras

List all running (and stopped) mock camera containers.

**Response `200`**
```json
[
  {
    "camId":         "parking-lot",
    "containerName": "mock-cam-parking-lot",
    "rtspUrl":       "rtsp://localhost:8554/cam/parking-lot",
    "status":        "Up 3 minutes"
  }
]
```

> `status` is the raw Docker container status string (e.g. `"Up 5 minutes"`, `"Exited (0) 2 minutes ago"`).

---

### POST /cameras

Create and start a new mock camera. The camera will loop the given video file and stream it over RTSP.

**Request**
```json
{
  "camId":     "parking-lot",
  "videoFile": "parking-lot.mp4"
}
```

- `camId` — unique identifier for the camera; used as the RTSP path segment and the container name suffix.
- `videoFile` — filename (not full path) of an already-uploaded video. Must exist in `/videos`.

**Response `201`**
```json
{
  "camId":         "parking-lot",
  "containerName": "mock-cam-parking-lot",
  "videoFile":     "parking-lot.mp4",
  "rtspUrl":       "rtsp://localhost:8554/cam/parking-lot",
  "hlsUrl":        "http://localhost:8888/cam/parking-lot"
}
```

> `hlsUrl` is currently hardcoded to `localhost:8888`. This will be made configurable in a future update.

**Errors**
| Status | Reason |
|--------|--------|
| `400`  | Missing `camId` or `videoFile` |
| `404`  | `videoFile` not found in `/videos` |
| `500`  | Docker container could not be created or started |

---

### DELETE /cameras/{camId}

Stop and remove a mock camera container.

**Example**
```
DELETE /cameras/parking-lot
```

**Response `200`**
```json
{ "deleted": "parking-lot" }
```

**Errors**
| Status | Reason |
|--------|--------|
| `404`  | Container not found or already stopped |
| `500`  | Container could not be removed |

---

## Typical workflow

```
1. POST /videos          — upload the video file you want to stream
2. POST /cameras         — create a camera that streams that file
3. GET  /cameras         — confirm it is running and grab the RTSP/HLS URLs
4. DELETE /cameras/{id}  — tear it down when done
```

---

## URL scheme

| Protocol | Pattern | Example |
|----------|---------|---------|
| RTSP     | `rtsp://<host>:<port>/cam/<camId>` | `rtsp://localhost:8554/cam/parking-lot` |
| HLS      | `http://localhost:8888/cam/<camId>` | `http://localhost:8888/cam/parking-lot` (hardcoded for now) |
