import logging
from datetime import datetime

def set_logger(logger_name: str):
    """
    :param logger_name: a string is used to name the log file
    :return: a customer made logger that logs to terminal as well as log files
    """
    filename = f"{logger_name}_{datetime.today().strftime('%Y_%b_%d_%H:%M:%S')}.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%b-%d %H:%M:%S')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

if __name__ == "__main__":
    print('')