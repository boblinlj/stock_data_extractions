from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from util.parallel_process import non_parallel_process
from modules.extract_factors import CalculateFactors
from sqlalchemy import create_engine
from datetime import date
import pandas as pd
import time


class FactorJob:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    sql = """
            with pop as (
                select distinct ticker
                from model_1_factors
                where updated_dt='{}'
            )
                SELECT DISTINCT a.yahoo_ticker as ticker
                FROM stock_list_for_cooble_stone a
                left join pop b
                    on a.ticker = b.ticker
                WHERE active_ind='A' and b.ticker is null
                ORDER BY 1
        """

    no_of_db_entries = 0

    def __init__(self, start_dt, updated_dt, batch_run=False, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.stock_list_df = pd.read_sql(con=self.cnn, sql=self.sql.format(self.updated_dt))
        self.start_dt = start_dt
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='factor_calc', loggerFileName=self.loggerFileName)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        each_stock = CalculateFactors(stock,
                                      start_dt=self.start_dt,
                                      updated_dt=self.updated_dt,
                                      loggerFileName=self.loggerFileName)
        stock_df = each_stock.run_pipeline()
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock} due to the dataframe is empty")
        else:
            if self._enter_db(stock_df, 'model_1_factors'):
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
        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))
