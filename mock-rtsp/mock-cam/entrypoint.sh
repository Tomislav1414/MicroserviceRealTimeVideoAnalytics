#!/bin/bash
set -e

echo "[mock-cam] CAM_ID=$CAM_ID | VIDEO=$VIDEO_PATH | TARGET=rtsp://$RTSP_HOST:$RTSP_PORT/$CAM_ID"

# Wait for the video file to exist
until [ -f "$VIDEO_PATH" ]; do
  echo "[mock-cam] Waiting for video file: $VIDEO_PATH"
  sleep "$RETRY_DELAY"
done

# Wait for MediaMTX to be reachable
until nc -z "$RTSP_HOST" "$RTSP_PORT" 2>/dev/null; do
  echo "[mock-cam] Waiting for MediaMTX at $RTSP_HOST:$RTSP_PORT..."
  sleep "$RETRY_DELAY"
done

echo "[mock-cam] MediaMTX is up. Starting stream..."

# OUTPUT_FPS is optional and unset by default: without it, ffmpeg passes
# through the source file's native frame rate untouched. Only set it for a
# camera whose source clip's native rate doesn't match the pipeline's
# designed 12fps (decoder/detector throughput is sized for 12fps per
# camera) — a mismatched source can 5x the real message rate and overload
# decode/detection for every camera sharing that infrastructure.
FPS_ARGS=()
if [ -n "$OUTPUT_FPS" ]; then
  FPS_ARGS=(-r "$OUTPUT_FPS")
fi

# Loop forever — if ffmpeg crashes, restart after delay
while true; do
  ffmpeg \
    -re \
    -stream_loop -1 \
    -i "$VIDEO_PATH" \
    -c:v libx264 \
    -preset ultrafast \
    -tune zerolatency \
    "${FPS_ARGS[@]}" \
    -g 30 \
    -c:a aac \
    -f rtsp \
    -rtsp_transport tcp \
    "rtsp://$RTSP_HOST:$RTSP_PORT/$CAM_ID" \
    && break   # exit loop cleanly if ffmpeg exits with 0

  echo "[mock-cam] ffmpeg exited, retrying in $RETRY_DELAY seconds..."
  sleep "$RETRY_DELAY"
done