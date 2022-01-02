import datetime
import logging
import os

import numpy as np

from configs import job_configs as jcfg
import math


def dedup_list(ls: list):
    return list(dict.fromkeys(ls))


def returnNotMatches(a, b):
    return [x for x in a if x not in b]


def regular_time_to_unix(date):
    if date is None:
        return np.nan
    else:
        return int((date - datetime.date(1970, 1, 1)).total_seconds())


def unix_to_regular_time(unix: int):
    if math.isnan(unix):
        return np.nan
    else:
        return datetime.datetime.utcfromtimestamp(unix).strftime('%Y-%m-%d')


def create_log(loggerName=__name__, loggerFileName=None):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(jcfg.LOG_FORMATTER)

    if loggerFileName is None:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    else:
        file_handler = logging.FileHandler(os.path.join(jcfg.JOB_ROOT, 'logs', f'{loggerFileName}'))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
