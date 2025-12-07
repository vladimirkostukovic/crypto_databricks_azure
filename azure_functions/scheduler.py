from datetime import datetime
from typing import List


def get_intervals_to_ingest() -> List[str]:
    now = datetime.utcnow()
    minutes = now.hour * 60 + now.minute

    intervals = []

    # 15m: every 15 minutes (00, 15, 30, 45)
    if minutes % 15 == 0:
        intervals.append("15m")

    # 1h: every hour (XX:00)
    if now.minute == 0:
        intervals.append("1h")

    # 4h: every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
    if now.minute == 0 and now.hour % 4 == 0:
        intervals.append("4h")

    # 1d: once a day (00:00)
    if now.hour == 0 and now.minute == 0:
        intervals.append("1d")

    return intervals