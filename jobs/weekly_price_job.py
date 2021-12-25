from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from helper import *
from util.parallel_process import parallel_process
from util.parallel_process import non_parallel_process
from util.helper_functions import create_log
import yahoo_price_for_backtesting as calc_mod
from sqlalchemy import create_engine
from datetime import date
import pandas as pd
import time
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

today = date.today()

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
# file_handler = logging.FileHandler('/logs/yahoo_target_price_{}.log'.format(today.strftime("%b-%d-%Y")))

file_handler = logging.FileHandler(os.path.join(jcfg.JOB_ROOT, 'logs', 'yahoo_price_{}.log'.format(today.strftime("%b-%d-%Y"))))
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class PriceJob:
    database_user = dbcfg.MYSQL_USER
    database_ip = dbcfg.MYSQL_HOST
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    sql = """
            SELECT DISTINCT yahoo_ticker as ticker
            FROM stock_list_for_cooble_stone 
            WHERE active_ind='A'
            ORDER BY 1
        """

    no_of_db_entries = 0

    def __init__(self, start_dt, updated_dt, batch_run=False, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.stock_list_df = pd.read_sql(con=self.cnn, sql=self.sql)
        self.start_dt = start_dt
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName

        if self.loggerFileName is not None:
            self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)
        else:
            self.logger = create_log(loggerName='YahooStats', loggerFileName=None)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        each_stock = calc_mod.YahooPrice(stock, self.start_dt, self.updated_dt, self.updated_dt)
        stock_df = each_stock.get_each_stock_price_from_yahoo_chart()
        if stock_df.empty:
            self.logger.info(f"Failed:Processing stock = {stock}")
        else:
            self.logger.info(f"Success: Processing stock = {stock}")
            if self._enter_db(stock_df, 'price'):
                self.logger.info(f"Success: Entered stock = {stock}")
            else:
                self.logger.info(f"Failed: Entering stock = {stock}")

    def _enter_db(self, df, table):
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=True, method='multi', chunksize=200)
            return True
        except Exception as e:
            self.logger.debug(e)
            return False

    def run_job(self):
        start = time.time()
        stock_list = self.stock_list_df['ticker'].to_list()[:]
        self.logger.info(f'There are {len(stock_list)} stocks to be extracted')

        if self.batch_run:
            parallel_process(stock_list, self._run_each_stock, self.workers)
        else:
            non_parallel_process(stock_list, self._run_each_stock, self.workers)
        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    obj = PriceJob(date(2000, 1, 1), date.today(), batch_run=True)
    # obj = PriceJob(datetime.date(2000, 1, 1), datetime.date(2021, 12, 18), batch_run=True)
    obj.run_job()
