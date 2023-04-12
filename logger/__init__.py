import logging

_logs = {}


def get_logger(logger_name: str):
    global _logs
    if _logs.get(logger_name):
        return _logs.get(logger_name)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s - LINE: %(lineno)d')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    _logs[logger_name] = logger
    return logger
