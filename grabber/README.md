# grabber

Reads each configured camera's RTSP stream, extracts H264 access units
(NAL units) without decoding them, and publishes them to Kafka. This is
stage 2 of the pipeline (mediamtx → **grabber** → decoder → detectors → ...).

Decoding is deliberately not done here: the grabber only depacketizes RTP
into NAL units (via [gortsplib](https://github.com/bluenviron/gortsplib)),
keeping this stage I/O-bound and cheap to run per camera. The stateful
decoder service (next pipeline stage) is where actual H264 decoding happens.

---

## Requires

- `infra` compose stack running (Kafka reachable at `kafka:9092` on the
  `vms-local` network).
- `mock-rtsp` compose stack running (cameras publishing to MediaMTX).

## Run

```bash
docker compose up --build
```

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `CAMERAS` | `cam/hum_det,cam/car_passing` | Comma-separated camera IDs (must match paths published to MediaMTX). Static list — no dynamic discovery. |
| `MEDIAMTX_HOST` | `mediamtx` | RTSP source host. |
| `MEDIAMTX_PORT` | `8554` | RTSP source port. |
| `KAFKA_BROKERS` | `kafka:9092` | Comma-separated Kafka bootstrap brokers. |
| `KAFKA_TOPIC` | `raw-frames` | Topic all cameras publish to. |
| `KAFKA_PARTITIONS` | number of cameras | Created on startup if the topic doesn't exist yet. Must be ≥ camera count — otherwise two cameras can share a partition and lose per-camera ordering. |

## Kafka message format

- **Key**: camera ID (e.g. `cam/hum_det`) — ensures all frames from one
  camera hash to the same partition, preserving GOP order for the decoder.
- **Value**: raw H264 access unit, NAL units concatenated with Annex-B
  start codes (`00 00 00 01`) — standard elementary-stream framing, decodable
  directly by ffmpeg/libav.
- **Headers**: `frame_id` (per-camera monotonic counter, resets on grabber
  restart), `captured_at_unix_nano` (grabber wall-clock receive time, for
  end-to-end latency measurement), `rtp_pts_ns` (stream's own RTP clock,
  not comparable across cameras), `is_keyframe`, `codec`.

Metadata is carried in headers rather than a JSON envelope so the payload
stays raw NAL bytes with no extra encode/decode cost per frame.

## Behavior notes

- Publishes full GOP (all I/P/B frames), not keyframe-only.
- Waits for the first keyframe on connect before publishing anything, so a
  camera's first published frame is always a valid decoder entry point.
- Reconnects with a fixed delay on any RTSP or Kafka error.
- Handles SIGTERM/SIGINT: stops camera workers and flushes the Kafka writer
  before exiting.
