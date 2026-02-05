import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from binance_core import run
from scheduler import get_intervals_to_ingest


def main(mytimer):
    logging.info("Binance timer triggered")
    intervals = get_intervals_to_ingest()

    if intervals:
        logging.info(f"Ingesting intervals: {', '.join(intervals)}")
        run(intervals=intervals)
    else:
        logging.info("No intervals to ingest at this time")
