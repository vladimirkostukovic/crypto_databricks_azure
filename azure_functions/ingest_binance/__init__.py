import logging
from .binance_core import run

def main(mytimer):
    logging.info("Binance timer triggered")
    run()