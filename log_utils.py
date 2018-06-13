import logging
import logging.handlers
import os
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger()

def init_log(log_path, level=logging.INFO, when="d", backup=24,
             format="%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s",
             datefmt="%m-%d %H:%M:%S"):
    formatter = logging.Formatter(format, datefmt)
    logger.setLevel(level)

    dir = os.path.dirname(log_path)
    if not os.path.isdir(dir):
        os.makedirs(dir)

    handler=logging.handlers.TimedRotatingFileHandler(log_path+".log",
            when=when,
            backupCount=backup)
    #handler = logging.FileHandler(log_path + ".log")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler=logging.handlers.TimedRotatingFileHandler(log_path+".log.wf",
            when=when,
            backupCount=backup)
    #handler = logging.FileHandler(log_path + ".log.wf")
    handler.setLevel(logging.WARNING)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def d(msg, *args, **kwargs):
    logger.debug(msg, *args, **kwargs)


def i(msg, *args, **kwargs):
    logger.info(msg, *args, **kwargs)


def w(msg, *args, **kwargs):
    logger.warning(msg, *args, **kwargs)


def e(msg, *args, **kwargs):
    logger.error(msg, *args, **kwargs)


def f(msg, *args, **kwargs):
    logger.critical(msg, *args, **kwargs)
