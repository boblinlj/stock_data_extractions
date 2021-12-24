import logging
import os
import datetime


logger = logging.getLogger('daily_job')
logger.setLevel(logging.DEBUG)
today = datetime.date.today()
formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler(
    os.path.join(jcfg.JOB_ROOT, 'logs', 'yahoo_stats_{}.log'.format(today.strftime("%b-%d-%Y"))))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
