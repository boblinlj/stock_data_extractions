from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from sqlalchemy import create_engine
from util.helper_functions import create_log
from modules.extract_analysis import YahooAnalysis
from util.parallel_process import parallel_process
from util.create_output_sqls import write_insert_db
from util.gcp_functions import upload_to_bucket
import os
import pandas as pd
import time
from datetime import date


class AnalysisJob:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER

    cnn = create_engine(
        f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
        pool_size=20,
        max_overflow=0)

    sql = """
            with pop as (
            SELECT ticker
            FROM finviz_tickers
            where updated_dt = '{}' 
            union
            select yahoo_ticker as ticker
            FROM stock_list_for_cooble_stone
            where active_ind='A'
            order by ticker)
            
            select distinct a.ticker 
            from pop a
            left join yahoo_consensus b
                on a.ticker = b.ticker
            where b.ticker is null
        """

    no_of_db_entries = 0
    failed_extraction = []

    def __init__(self, updated_dt, batch_run=True, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.stock_list_df = pd.read_sql(con=self.cnn, sql=self.sql.format(self.updated_dt))
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='yahoo_analysis', loggerFileName=self.loggerFileName)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        stock_df = YahooAnalysis(stock, loggerFileName).get_analysis_data()
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock} due to the dataframe is empty")
        else:
            stock_df['updated_dt'] = self.updated_dt
            if self._enter_db(stock_df, 'yahoo_consensus'):
                self.logger.info(f"Success: Entered stock = {stock}")
            else:
                self.logger.debug(f"Failed: Entering stock = {stock}")

    def _enter_db(self, df, table):
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=False, method='multi', chunksize=200)
            return True
        except Exception as e:
            self.logger.debug(e)
            return False

    def run_job(self):
        start = time.time()
        stock_list = self.stock_list_df['ticker'].to_list()[:]
        # print(stock_list)
        self.logger.info(f'There are {len(stock_list)} stocks to be extracted')
        if self.batch_run:
            parallel_process(stock_list, self._run_each_stock, n_jobs=self.workers)
        else:
            parallel_process(stock_list, self._run_each_stock, n_jobs=1)

        self.logger.info(f"-----Start generate SQL outputs-----")
        insert = write_insert_db('yahoo_analysis', self.updated_dt)
        insert.run_insert()

        self.logger.info(f"-----Upload SQL outputs to GCP-----")
        file = f'insert_yahoo_analysis_factors_{self.updated_dt}.sql'
        if upload_to_bucket(file, os.path.join(jcfg.JOB_ROOT, "sql_outputs", file), 'stock_data_busket2'):
            self.logger.info("GCP upload successful for file = {}".format(file))
        else:
            self.logger.debug("Failed: GCP upload failed for file = {}".format(file))

        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    loggerFileName = f"weekly_yahoo_conesus_{date.today().strftime('%Y%m%d')}.log"

    obj = AnalysisJob(date(2021, 12, 24),
                      batch_run=True,
                      loggerFileName=loggerFileName)
    obj.run_job()
