from datetime import datetime
from typing import List


def get_intervals_to_ingest() -> List[str]:
    now = datetime.utcnow()
    minutes = now.hour * 60 + now.minute

    intervals = []

    if minutes % 15 == 0:
        intervals.append("15m")

    if now.minute == 0:
        intervals.append("1h")

    if now.minute == 0 and now.hour % 4 == 0:
        intervals.append("4h")

    if now.hour == 0 and now.minute == 0:
        intervals.append("1d")

    return intervals
