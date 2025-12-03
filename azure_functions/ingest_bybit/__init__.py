import logging
from .bybit_core import run

def main(mytimer):
    logging.info("Bybit TimerTrigger started")
    run()