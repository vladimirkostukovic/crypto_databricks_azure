import logging
from azure.functions import TimerRequest

from azure_functions.ingest_binance import ingest_binance
from azure_functions.ingest_bybit import ingest_bybit

def main(mytimer: TimerRequest):
    logging.info("Starting scheduled ingestion...")

    # Binance
    try:
        ingest_binance()
        logging.info("Binance OK")
    except Exception as e:
        logging.error(f"Binance FAILED: {e}")

    # Bybit
    try:
        ingest_bybit()
        logging.info("Bybit OK")
    except Exception as e:
        logging.error(f"Bybit FAILED: {e}")

    logging.info("Ingestion cycle completed.")