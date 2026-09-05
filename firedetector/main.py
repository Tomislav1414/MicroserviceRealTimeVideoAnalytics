import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

import torch
from ultralytics import YOLO

from detectorsdk import DetectionProducer, FrameConsumer, bbox

CONFIDENCE_FLOOR = float(os.getenv("CONFIDENCE_FLOOR", "0.4"))
DEVICE = os.getenv("DEVICE") or None
SAMPLE_EVERY_N_FRAMES = int(os.getenv("SAMPLE_EVERY_N_FRAMES", "1"))
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fire-detector")
CAMERAS = {c.strip() for c in os.getenv("CAMERAS", "").split(",") if c.strip()}


def main() -> None:
    print(f"cuda available: {torch.cuda.is_available()}, device={DEVICE or 'auto'}", file=sys.stderr)
    model = YOLO("best.pt")
    print(f"model classes: {model.names}", file=sys.stderr)
    producer = DetectionProducer(detector="fire")
    consumer = FrameConsumer(group_id=KAFKA_GROUP_ID, sample_every_n=SAMPLE_EVERY_N_FRAMES)

    signal.signal(signal.SIGTERM, lambda *_: consumer.close())
    signal.signal(signal.SIGINT, lambda *_: consumer.close())

    print(f"Consuming decoded frames as group={KAFKA_GROUP_ID}, sample_every_n={SAMPLE_EVERY_N_FRAMES}")

    try:
        for frame in consumer.frames():
            if CAMERAS and frame.cam_id not in CAMERAS:
                continue

            results = model(
                frame.image,
                conf=CONFIDENCE_FLOOR,
                device=DEVICE,
                verbose=False,
            )

            detections = []
            for result in results:
                for box in result.boxes:
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    label = model.names[int(box.cls[0])]
                    detections.append(bbox(x1, y1, x2, y2, box.conf[0], label=label))
            producer.send(cam_id=frame.cam_id, detections=detections, ts=frame.captured_at)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
