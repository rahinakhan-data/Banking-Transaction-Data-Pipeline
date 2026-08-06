import os
import logging

parent_dir = os.getcwd()
log_dir = os.path.join(parent_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, 'etl.log')

def etl_logger():

    # set the format of log : Time - Status - Message
    log_format = " %(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level = logging.INFO,
        format = log_format,
        datefmt = date_format,
        handlers = [logging.FileHandler(log_file_path), logging.StreamHandler()]
    )
    return logging.getLogger("ETL _LOgger")

logger = etl_logger()
