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

# Loop forever — if ffmpeg crashes, restart after delay
while true; do
  ffmpeg \
    -re \
    -stream_loop -1 \
    -i "$VIDEO_PATH" \
    -c:v libx264 \
    -preset ultrafast \
    -tune zerolatency \
    -g 30 \
    -c:a aac \
    -f rtsp \
    -rtsp_transport tcp \
    "rtsp://$RTSP_HOST:$RTSP_PORT/$CAM_ID" \
    && break   # exit loop cleanly if ffmpeg exits with 0

  echo "[mock-cam] ffmpeg exited, retrying in $RETRY_DELAY seconds..."
  sleep "$RETRY_DELAY"
done