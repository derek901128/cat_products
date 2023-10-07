import logging
from datetime import datetime
from pathlib import Path

def set_logger(logger_name: str):
    filename = f"{logger_name}_{datetime.today().strftime('%Y_%b_%d_%H:%M:%S')}.log"
    project_path = Path('..')
    loggers_path = project_path.joinpath('loggers')

    if not loggers_path.joinpath(logger_name).exists():
        loggers_path.joinpath(logger_name).mkdir(exist_ok=True)

    log_path = loggers_path.joinpath(logger_name)
    log_fullpath = log_path / Path(filename)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%b-%d %H:%M:%S')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_fullpath.as_posix())
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

if __name__ == "__main__":
    print('')
