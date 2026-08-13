import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import cv2
from confluent_kafka import Consumer


@dataclass
class Frame:
    cam_id: str
    image: np.ndarray
    frame_id: int
    captured_at: datetime
    is_keyframe: bool


class FrameConsumer:
    """Consumes JPEG frames published by the decoder service, decodes them,
    and applies a per-camera sampling rate before handing them to the caller.

    Sampling is driven by the `frame_id` header (a monotonic per-camera
    counter set by the grabber), not a local counter — frame_id is already
    scoped per camera, so this stays correct even when one consumer process
    is assigned partitions for multiple cameras.
    """

    def __init__(
        self,
        group_id: str,
        topic: str | None = None,
        bootstrap_servers: str | None = None,
        sample_every_n: int = 1,
        **consumer_config,
    ):
        self._topic = topic or os.environ.get("DECODED_FRAMES_TOPIC", "decoded-frames")
        bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        self._sample_every_n = max(1, sample_every_n)
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                **consumer_config,
            }
        )
        self._consumer.subscribe([self._topic])
        self._closed = False

    def frames(self):
        """Yields a Frame for every sampled message. Stops once close() has
        been called from another thread/signal handler.
        """
        while not self._closed:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka consume error: {msg.error()}", file=sys.stderr)
                continue

            headers = dict(msg.headers() or [])
            frame_id = int(headers.get("frame_id", b"0"))
            if frame_id % self._sample_every_n != 0:
                continue

            image = cv2.imdecode(np.frombuffer(msg.value(), dtype=np.uint8), cv2.IMREAD_COLOR)
            if image is None:
                print(f"Failed to decode JPEG for frame_id={frame_id}, skipping", file=sys.stderr)
                continue

            captured_at_ns = int(headers.get("captured_at_unix_nano", b"0"))
            yield Frame(
                cam_id=msg.key().decode(),
                image=image,
                frame_id=frame_id,
                captured_at=datetime.fromtimestamp(captured_at_ns / 1e9, tz=timezone.utc),
                is_keyframe=headers.get("is_keyframe", b"false") == b"true",
            )

    def close(self) -> None:
        self._closed = True
        self._consumer.close()
