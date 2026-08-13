# decoder

Consumes `raw-frames` (published by [`grabber`](../grabber)), decodes H264
access units, encodes each decoded frame to JPEG, and republishes to
`decoded-frames`. This is stage 3 of the pipeline (grabber → **decoder** →
detectors → ...).

Decoding is the reason this stage is stateful and scales only with camera
count, not arbitrarily: H264 P/B frames depend on prior frames in the GOP,
so each camera's stream must be decoded in order by a single consumer. That
consumer is a whole Kafka partition, not a whole process — scaling is done
by running more replicas, and Kafka's normal consumer-group rebalancing
hands out partitions (cameras) across them. No custom partition-assignment
code is needed for this.

## Why cgo

H264 decoding needs a real decoder; there's no viable pure-Go one. This
service links FFmpeg's `libavcodec`/`libswscale` directly via cgo
(`avcodec_send_packet`/`avcodec_receive_frame`, same as
[gortsplib's own H264 decode example](https://github.com/bluenviron/gortsplib/tree/main/examples/client-play-format-h264)),
rather than shelling out to an `ffmpeg` subprocess. Direct bindings give an
exact 1:1 call correspondence between an input packet and its decoded
frame, so the frame's metadata (`frame_id`, capture timestamp) can be
carried straight through without re-deriving it from a re-parsed
byte stream. JPEG encoding, on the other hand, is plain Go (`image/jpeg`
from the standard library) — no reason to involve cgo for that half.

**Assumption**: this 1-packet-in-at-most-1-frame-out model holds because
the mock cameras encode with `bframes=0` (see `mock-rtsp/mock-cam/entrypoint.sh`).
With B-frames, libavcodec can buffer several packets before emitting a
frame, and this decoder does not currently handle that (it would just
silently under-produce frames rather than misattribute metadata).

## Resync on keyframe

A partition can be (re)assigned to a worker at any offset — after a
restart, that offset is wherever this consumer group last committed, not
necessarily a keyframe boundary — and every reassignment gets a brand new
`h264Decoder` with no reference frames in memory. So every partition
worker discards messages until it sees the next `is_keyframe` header,
regardless of why it was assigned (first time, rebalance, or restart).

## Run

Requires `infra` (Kafka) and `grabber` (producing `raw-frames`) running
first.

```bash
docker compose up --build
docker compose up --build --scale decoder=2   # one replica per camera, if there are 2
```

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `kafka:9092` | Comma-separated Kafka bootstrap brokers. |
| `KAFKA_GROUP_ID` | `decoder` | Consumer group ID — all replicas must share this. |
| `INPUT_TOPIC` | `raw-frames` | Topic written by the grabber. |
| `OUTPUT_TOPIC` | `decoded-frames` | Topic this service writes to. Created on startup with the same partition count as `INPUT_TOPIC`. |
| `JPEG_QUALITY` | `85` | `image/jpeg` quality (1-100). |
| `COMMIT_EVERY` | `25` | Commit the consumer group offset every N processed frames (~once/second at the mock cameras' framerate), plus once more on generation end. |

## Behavior notes

- On SIGTERM/SIGINT, at most one in-flight frame per partition can fail to
  send (logged as `kafka send error: consumer group generation has ended`)
  because the send races the consumer group generation ending. Exit code is
  still 0. Acceptable for a real-time video pipeline — not worth the added
  complexity of a separate shutdown grace window to avoid losing one frame.

## Kafka message format (`decoded-frames`)

- **Key**: camera ID, passed through unchanged from `raw-frames`.
- **Partition**: same index as the source `raw-frames` partition — decoder
  writes with an explicit passthrough balancer, not a hash, so this is a
  guarantee, not a probability (see `grabber/README.md` for why that
  distinction mattered in practice).
- **Value**: JPEG-encoded frame.
- **Headers**: `frame_id`, `captured_at_unix_nano`, `rtp_pts_ns` (all
  propagated from `raw-frames`, for end-to-end latency), plus
  `decoded_at_unix_nano` (this service's own processing time, for
  per-stage latency), `is_keyframe`, `codec: jpeg`.
