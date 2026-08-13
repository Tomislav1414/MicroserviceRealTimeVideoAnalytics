import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer


class DetectionProducer:
    def __init__(self, detector: str, topic: str | None = None, bootstrap_servers: str | None = None, **producer_config):
        self._detector = detector
        self._topic = topic or os.environ.get("KAFKA_TOPIC") or f"{detector}-detections"
        bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        self._producer = Producer({"bootstrap.servers": bootstrap_servers, **producer_config})

    def send(self, cam_id: str, detections: list[dict], ts: datetime | None = None) -> None:
        payload = {
            "detector": self._detector,
            "cam_id": cam_id,
            "ts": (ts or datetime.now(timezone.utc)).isoformat(),
            "detection_count": len(detections),
            "detections": detections,
        }
        self._producer.produce(
            self._topic,
            key=cam_id,
            value=json.dumps(payload),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)


def _on_delivery(err, msg):
    if err:
        print(f"Kafka delivery failed [{msg.topic()}]: {err}")
