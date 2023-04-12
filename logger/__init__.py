import logging


def init_logs(logger_name: str):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s - LINE: %(lineno)d')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
