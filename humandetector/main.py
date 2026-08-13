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
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "human-detector")

# COCO class 0 = person
PERSON_CLASS_ID = 0


def main() -> None:
    print(f"cuda available: {torch.cuda.is_available()}, device={DEVICE or 'auto'}", file=sys.stderr)
    model = YOLO("yolo11n.pt")
    producer = DetectionProducer(detector="human")
    consumer = FrameConsumer(group_id=KAFKA_GROUP_ID, sample_every_n=SAMPLE_EVERY_N_FRAMES)

    signal.signal(signal.SIGTERM, lambda *_: consumer.close())
    signal.signal(signal.SIGINT, lambda *_: consumer.close())

    print(f"Consuming decoded frames as group={KAFKA_GROUP_ID}, sample_every_n={SAMPLE_EVERY_N_FRAMES}")

    try:
        for frame in consumer.frames():
            results = model(
                frame.image,
                classes=[PERSON_CLASS_ID],
                conf=CONFIDENCE_FLOOR,
                device=DEVICE,
                verbose=False,
            )

            detections = []
            for result in results:
                for box in result.boxes:
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    detections.append(bbox(x1, y1, x2, y2, box.conf[0]))
            # ts=frame.captured_at (grabber's capture time), not "now" — keeps
            # RisingWave's event-time sessionization accurate regardless of
            # how long YOLO inference itself takes.
            producer.send(cam_id=frame.cam_id, detections=detections, ts=frame.captured_at)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
