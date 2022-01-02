from configs import job_configs as jcfg
from util.parallel_process import parallel_process
from util.helper_functions import create_log, dedup_list, returnNotMatches
from util.create_output_sqls import write_insert_db
from util.gcp_functions import upload_to_bucket
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.get_stock_population import StockPopulation
from modules.extract_yahoo_price import YahooPrice
from datetime import date
import time
import os


class PriceJob:
    stock_lst = StockPopulation()
    workers = jcfg.WORKER

    def __init__(self, start_dt, updated_dt, batch_run=False, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.start_dt = start_dt
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        each_stock = YahooPrice(stock=stock,
                                start_dt=self.start_dt,
                                end_dt=self.updated_dt,
                                interval='1d',
                                includePrePost='false',
                                loggerFileName=self.loggerFileName)

        stock_df = each_stock.get_detailed_stock_price()
        stock_df['updated_dt'] = self.updated_dt
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock}")
        else:
            try:
                DatabaseManagement(data_df=stock_df, table='price', insert_index=True).insert_db()
                self.logger.info(f"Success: Entered stock = {stock}")
            except DatabaseManagementError as e:
                self.logger.debug(f"Failed: Entering stock = {stock}, {e}")

    def run_job(self):
        start = time.time()
        stocks = self.stock_lst.get_stock_list_from_arron()

        stocks = dedup_list(stocks)
        existing_rec = DatabaseManagement(table='price',
                                          key='ticker',
                                          where=f"updated_dt = '{self.updated_dt}'"
                                          ).check_population()
        stocks = returnNotMatches(stocks, existing_rec)[:]

        self.logger.info(f'There are {len(stocks)} stocks to be extracted')

        if self.batch_run:
            parallel_process(stocks, self._run_each_stock, self.workers)
        else:
            parallel_process(stocks, self._run_each_stock, 1)

        self.logger.info(f"-----Start generate SQL outputs-----")
        insert = write_insert_db('price', self.updated_dt)
        insert.run_insert()

        self.logger.info(f"-----Upload SQL outputs to GCP-----")
        file = f'insert_price_{self.updated_dt}.sql'
        if upload_to_bucket(file, os.path.join(jcfg.JOB_ROOT, "sql_outputs", file), 'stock_data_busket2'):
            self.logger.info("GCP upload successful for file = {}".format(file))
        else:
            self.logger.debug("Failed: GCP upload failed for file = {}".format(file))

        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    loggerFileName = f"daily_yahoo_price_{date.today().strftime('%Y%m%d')}.log"

    obj = PriceJob(start_dt=date(2000, 1, 1),
                   updated_dt=date(2021, 12, 31),
                   batch_run=True,
                   loggerFileName=loggerFileName)
    obj.run_job()

