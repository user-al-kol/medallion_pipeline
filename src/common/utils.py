import logging
import os

def logger(level, log_file):
    LOG_DIR = "/app/logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    log_path = os.path.join(LOG_DIR, log_file)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),          # stdout
            logging.FileHandler(log_path)     # file
        ]
    )
