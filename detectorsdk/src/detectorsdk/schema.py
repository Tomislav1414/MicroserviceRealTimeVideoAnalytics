def bbox(x1: float, y1: float, x2: float, y2: float, confidence: float, **extra) -> dict:
    return {
        "x1": int(x1),
        "y1": int(y1),
        "x2": int(x2),
        "y2": int(y2),
        "confidence": round(float(confidence), 4),
        **extra,
    }
