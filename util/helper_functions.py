import datetime
import logging
from logging.handlers import MemoryHandler
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


def check_existing_entries(df_to_check, existing_df):
    df_after_check = df_to_check[~df_to_check.index.isin(existing_df.index, level=0)]
    return df_after_check


def create_log(loggerName=__name__, loggerFileName=None, disable_log=False):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(jcfg.LOG_FORMATTER)

    if loggerFileName is None:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    else:
        file_handler = logging.FileHandler(os.path.join(jcfg.JOB_ROOT, 'logs', f'{loggerFileName}'))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if disable_log:
        logger.disabled = True

    return logger


def log_with_buffer(logger, target_handler=None, flush_level=None, capacity=None):
    if target_handler is None:
        target_handler = logging.StreamHandler()
    if flush_level is None:
        flush_level = logging.ERROR
    if capacity is None:
        capacity = 100
    handler = MemoryHandler(capacity, flushLevel=flush_level, target=target_handler)

    def decorator(fn):
        def wrapper(*args, **kwargs):
            logger.addHandler(handler)
            try:
                return fn(*args, **kwargs)
            except Exception:
                logger.exception('call failed')
                raise
            finally:
                super(MemoryHandler, handler).flush()
                logger.removeHandler(handler)
        return wrapper

    return decorator


def dedupe_dataframe(input_dataframe):
    return input_dataframe.groupby(input_dataframe.index).first()
