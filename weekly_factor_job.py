from configs import job_configs as jcfg
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from modules.extract_factors import CalculateFactors
from datetime import date, timedelta
import time
import os
from util.gcp_functions import upload_to_bucket
from util.create_output_sqls import write_insert_db
from util.get_stock_population import SetPopulation
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.helper_functions import returnNotMatches


class FactorJob:

    workers = jcfg.WORKER

    no_of_db_entries = 0

    def __init__(self, start_dt, updated_dt, targeted_table, targeted_pop, batch_run=True, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.start_dt = start_dt
        self.targeted_table = targeted_table
        self.targeted_pop = targeted_pop
        self.batch_run = batch_run
        # add existing population and extraction population
        self.existing_list = DatabaseManagement(table=self.targeted_table,
                                                key='ticker',
                                                where=f"updated_dt='{self.updated_dt}'").check_population()
        self.stock_list = SetPopulation(self.targeted_pop).setPop()
        # Add logger info
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='factor_calc', loggerFileName=self.loggerFileName)

    def _calculate_final_pop(self):
        return returnNotMatches(self.stock_list,
                                self.existing_list + jcfg.BLOCK)

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        # print(f"Start Processing stock = {stock}")
        each_stock = CalculateFactors(stock=stock,
                                      start_dt=self.start_dt,
                                      updated_dt=self.updated_dt,
                                      loggerFileName=self.loggerFileName)
        try:
            stock_df = each_stock.run_pipeline()
            if stock_df.empty:
                self.logger.debug(f"Failed:Processing stock = {stock} due to the dataframe is empty")
            else:
                try:
                    DatabaseManagement(data_df=stock_df, table=self.targeted_table, insert_index=True).insert_db()
                    self.logger.info(f"Success: Entered stock = {stock}")
                except DatabaseManagementError as e:
                    self.logger.debug(f"Failed: Entering stock = {stock} as {e}")
        except Exception as e:
            self.logger.debug(f"failed to calculate factors for stock {stock} as {e}")

    def _write_sql_output(self):
        self.logger.info(f"{'-'*10}Start generate SQL outputs{'-'*10}")
        insert = write_insert_db(self.targeted_table, self.updated_dt)
        insert.run_insert()

    def _upload_to_gcp(self):
        self.logger.info(f"{'-'*10}Upload SQL outputs to GCP{'-'*10}")
        file = f'insert_{self.targeted_table}_{self.updated_dt}.sql'
        if upload_to_bucket(file, os.path.join(jcfg.JOB_ROOT, "sql_outputs", file), jcfg.GCP_BUSKET_NAME):
            self.logger.info("GCP upload successful for file = {}".format(file))
        else:
            self.logger.debug("Failed: GCP upload failed for file = {}".format(file))

    def job(self):
        stock_list = self._calculate_final_pop()
        # self.logger.info(f'There are {len(stock_list)} stocks to be extracted')
        print(f'There are {len(stock_list)} stocks to be extracted')
        if self.batch_run:
            parallel_process(stock_list, self._run_each_stock, self.workers)
        else:
            parallel_process(stock_list, self._run_each_stock, 1)

    def run(self):
        start = time.time()

        self.job()
        self._write_sql_output()
        self._upload_to_gcp()

        end = time.time()
        self.logger.info("Extraction took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    loggerFileName = f"weekly_model_1_factor_{date.today().strftime('%Y%m%d')}.log"

    obj = FactorJob(start_dt=date(2010, 1, 1),
                    updated_dt=date.today(),
                    targeted_table='model_1_factors',
                    targeted_pop='AARON',
                    batch_run=False,
                    loggerFileName=loggerFileName)
    obj.run()
