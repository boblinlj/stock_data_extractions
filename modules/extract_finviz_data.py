import pandas as pd
from configs import finviz_configs as fcfg
from configs import job_configs as jcfg
import time
from util.helper_functions import create_log
from util.request_website import FinvizParserPerPage
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.parallel_process import parallel_process


class Finviz:
    url_base = 'https://finviz.com/screener.ashx?v=152&ft=4'
    workers = jcfg.WORKER

    def __init__(self, updated_dt, batch=True, loggerFileName=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.batch = batch
        self.existing_data = DatabaseManagement(key='ticker, updated_dt',
                                                table='finviz',
                                                where=f"updated_dt = '{self.updated_dt}'").get_record()
        self.existing_data['updated_dt'] = pd.to_datetime(self.existing_data['updated_dt'])
        self.existing_data.set_index(['ticker'], inplace=True)
        self.logger = create_log(loggerName='Finviz', loggerFileName=self.loggerFileName)

    def finviz_url_builder(self, page_num):
        url_opt = '&c=' + ','.join(str(i) for i in range(1, 71))
        return self.url_base + f'&r={(page_num-1)*20+1}' + url_opt

    @staticmethod
    def _check_existing_entries(df_to_check, existing_df):
        df_after_check = df_to_check[~(df_to_check.index.isin(existing_df.index))]
        return df_after_check

    def _parse_each_page(self, page_num):
        self.logger.info(f'Parsing Page {page_num}')
        parser = FinvizParserPerPage(url=self.finviz_url_builder(page_num), proxy=False)
        content_df = parser.parse()
        if content_df.empty:
            return 0, None

        content_df['updated_dt'] = self.updated_dt
        content_df.set_index(['ticker'], inplace=True)
        final_df = self._check_existing_entries(content_df, self.existing_data)

        return parser.no_of_population, final_df

    def _process_each_page(self, page_num):
        n, stock_df = self._parse_each_page(page_num)
        if stock_df.empty:
            self.logger.debug(f"data is empty for page={page_num}")
            return None

        try:
            DatabaseManagement(data_df=stock_df[fcfg.FINVIZ_TICKERS],
                               table='finviz_tickers',
                               insert_index=True).insert_db()
            DatabaseManagement(data_df=stock_df[fcfg.FINVIZ_SCREENER],
                               table='finviz_screener',
                               insert_index=True).insert_db()
            self.logger.info(f'data process successfully for page={page_num} ')
        except DatabaseManagementError as e:
            self.logger.debug(f'data process error for page={page_num} as {e}')

    def parse_all_pages(self):
        no_of_pop, final_df = self._parse_each_page(1)
        total_pages = int(round(no_of_pop/20, 0))
        if self.batch:
            parallel_process([x+1 for x in range(total_pages)], self._process_each_page, self.workers)
        else:
            parallel_process([x+1 for x in range(total_pages)], self._process_each_page, 1)

    def run(self):
        start = time.time()
        self.parse_all_pages()
        end = time.time()
        self.logger.info("Job took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    # test
    finviz = Finviz('2022-01-04', batch=False, loggerFileName=None)
    finviz._process_each_page(1)
    # finviz.run()
