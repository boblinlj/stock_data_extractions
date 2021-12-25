from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from util.parallel_process import parallel_process
from util.parallel_process import non_parallel_process
from util.helper_functions import create_log
from util.create_output_sqls import write_insert_db
from util.gcp_functions import upload_to_bucket
from modules.extract_yahoo_price import YahooPrice
from sqlalchemy import create_engine
from datetime import date, timedelta
import pandas as pd
import time
import os


class PriceJob:
    database_user = dbcfg.MYSQL_USER
    database_ip = dbcfg.MYSQL_HOST
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER

    cnn = create_engine(
        f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
        pool_size=20,
        max_overflow=0)

    sql_stock_list = """
            SELECT DISTINCT yahoo_ticker as ticker
            FROM stock_list_for_cooble_stone 
            WHERE active_ind='A'
            ORDER BY 1
        """

    no_of_db_entries = 0

    def __init__(self, start_dt, updated_dt, batch_run=False, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.start_dt = start_dt
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName

        self.stock_list_df = pd.read_sql(con=self.cnn, sql=self.sql_stock_list)

        if self.loggerFileName is not None:
            self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)
        else:
            self.logger = create_log(loggerName='YahooStats', loggerFileName=None)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        each_stock = YahooPrice(stock,
                                self.start_dt,
                                self.updated_dt,
                                self.updated_dt,
                                loggerFileName=self.loggerFileName)
        stock_df = each_stock.get_detailed_stock_price()
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock}")
        else:
            if self._enter_db(stock_df, 'price'):
                self.logger.info(f"Success: Entered stock = {stock}")
            else:
                self.logger.debug(f"Failed: Entering stock = {stock}")

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

        self.logger.info(f"-----Start generate SQL outputs-----")
        insert = write_insert_db('price', self.updated_dt)
        insert.run_insert()

        self.logger.info(f"-----Upload SQL outputs to GCP-----")
        file = f'insert_price_{self.updated_dt}.log'
        if upload_to_bucket(file, os.path.join(jcfg.JOB_ROOT, "sql_outputs", file), 'stock_data_busket2'):
            self.logger.info("GCP upload successful for file = {}".format(file))
        else:
            self.logger.debug("Failed: GCP upload failed for file = {}".format(file))

        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    loggerFileName = f"daily_yahoo_price_{date.today().strftime('%Y%m%d')}.log"

    obj = PriceJob(start_dt=date.today() - timedelta(days=500),
                   updated_dt=date.today(),
                   batch_run=True,
                   loggerFileName=loggerFileName)
    obj.run_job()

