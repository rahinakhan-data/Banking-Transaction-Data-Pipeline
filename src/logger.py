import os
import logging
from config import LOGS_DIR

log_file_path = os.path.join(LOGS_DIR, 'etl.log')

def etl_logger():

    # set the format of log : Time - Status - Message
    log_format = "[%(asctime)s] - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level = logging.INFO,
        format = log_format,
        datefmt = date_format,
        handlers = [logging.FileHandler(log_file_path), logging.StreamHandler()]
    )
    return logging.getLogger()

logger = etl_logger()
