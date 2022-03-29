import time
import pandas as pd
import numpy as np
from util.helper_functions import dedup_list, create_log
import os
import datetime
from util.parallel_process import *
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.get_stock_population import SetPopulation

pd.set_option('display.max_columns', None)


class ReadYahooFinancialData:
    def __init__(self, js):
        self.js = js

    def parse(self):
        data_dic = {'12M': [],
                    '3M': [],
                    'TTM': []
                    }

        for each_result in self.js['timeseries']['result'][:]:
            var_name = each_result['meta']['type'][0]
            ticker = each_result['meta']['symbol'][0]

            if 'annual' in var_name: periodType = '12M'
            elif 'quarterly' in var_name: periodType = '3M'
            else: periodType = 'TTM'

            one_list = []

            if len(each_result) == 3:
                for data in each_result[var_name]:
                    one_list.append(
                        {"ticker": ticker,
                         var_name: data['reportedValue']['raw'],
                         'asOfDate': data['asOfDate']}
                    )
            else:
                one_list.append(
                    {"ticker": ticker,
                     var_name: np.NaN,
                     'asOfDate': np.NaN}
                )

            temp_df = pd.DataFrame.from_records(data=one_list)
            temp_df['asOfDate'] = pd.to_datetime(temp_df['asOfDate'])
            temp_df.set_index(['ticker', 'asOfDate'], inplace=True)

            data_dic[periodType].append(temp_df)

        df_12m = pd.concat(data_dic['12M'], axis=1, sort=True).dropna(how='all')
        df_3m = pd.concat(data_dic['3M'], axis=1, sort=True).dropna(how='all')
        df_ttm = pd.concat(data_dic['TTM'], axis=1, sort=True).dropna(how='all')

        return df_12m, df_3m, df_ttm


class YahooFinancial:
    BASE_URL = 'https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}'

    workers = jcfg.WORKER

    df_for_elements = pd.read_csv(os.path.join(jcfg.JOB_ROOT, 'inputs', 'yahoo_financial_fundamental.csv'))

    all_elements = df_for_elements.data.values.tolist()

    no_of_requests = 0
    no_of_db_entries = 0

    table_lookup = {'yahoo_quarterly_fundamental': 'quarter',
                    'yahoo_annual_fundamental': 'annual',
                    'yahoo_trailing_fundamental': 'ttm'}

    failed_extract = []

    def __init__(self, updated_dt, targeted_population,batch=False, loggerFileName=None, use_tqdm=True):
        self.updated_dt = updated_dt
        self.targeted_population = targeted_population
        self.loggerFileName = loggerFileName
        self.batch = batch
        self.logger = create_log(loggerName='YahooFinancialStatements', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm

    def _existing_dt(self):
        annual_data = DatabaseManagement(table='yahoo_annual_fundamental',
                                         key="ticker, asOfDate, 'annual' as type",
                                         where="1=1").get_record()

        quarter_data = DatabaseManagement(table='yahoo_quarterly_fundamental',
                                         key="ticker, asOfDate, 'quarter' as type",
                                         where="1=1").get_record()

        ttm_data = DatabaseManagement(table='yahoo_trailing_fundamental',
                                      key="ticker, asOfDate, 'ttm' as type",
                                      where="1=1").get_record()

        self.ext_list_data = pd.concat([annual_data, quarter_data, ttm_data], axis=0)
        self.ext_list_data['asOfDate'] = pd.to_datetime(self.ext_list_data['asOfDate'])

    def _url_builder_fundamentals(self):

        tdk = str(int(time.mktime(datetime.datetime.now().timetuple())))
        BASE_URL = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{stock}?symbol={stock}&type='
        URL_TAIL = '&merge=false&period1=493590046&period2=' + tdk
        url = ''
        for element in self.all_elements:
            url = url + element + '%2C'

        return BASE_URL + url + URL_TAIL

    def _extract_api(self, stock):
        url = self._url_builder_fundamentals().format(stock=stock)
        print(url)
        data = None
        for trail in range(5):
            data = YahooAPIParser(url=url).parse()
            if data is not None:
                break

        if data is None:
            self.logger.debug(f'unable to get yahoo API data for stock={stock}')
        else:
            return data

    def _extract_each_stock(self, stock):
        js = self._extract_api(stock)
        if js is None:
            self.failed_extract.append(stock)
            return None

        df_12m, df_3m, df_ttm = ReadYahooFinancialData(js).parse()

        self._insert_to_db(df_12m, stock, 'yahoo_annual_fundamental')
        self._insert_to_db(df_3m, stock, 'yahoo_quarterly_fundamental')
        self._insert_to_db(df_ttm, stock, 'yahoo_trailing_fundamental')

    def _insert_to_db(self, df, stock, table):

        df_to_insert = df.copy()
        df_to_insert['updated_dt'] = self.updated_dt
        df_to_insert = self._check_existing_entries_financial(df_to_check=df_to_insert, stock=stock, table=table)
        try:
            DatabaseManagement(df_to_insert, table=table, insert_index=True).insert_db()
        except DatabaseManagementError as e:
            self.logger.debug(f"Failed to insert data for stock={stock} as {e}")

    def _check_existing_entries_financial(self, df_to_check, stock, table):
        df_existing_data = self.ext_list_data[
                (self.ext_list_data['type'] == self.table_lookup[table]) &
                (self.ext_list_data['ticker'] == stock)
            ]
        df_existing_data.set_index(['ticker', 'asOfDate'], inplace=True)
        df_after_check = df_to_check[~df_to_check.index.isin(df_existing_data.index)]
        return df_after_check

    def run(self):
        start = time.time()
        self._existing_dt()

        stocks = SetPopulation(user_pop=self.targeted_population).setPop()
        # stocks = self.stock_lst.all_stocks_wo_ETF_RIET()[:]

        self.logger.info("-------------First Extract Starts-------------")
        self.logger.info("{} Stocks to be extracted".format(len(stocks)))
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info(self.failed_extract)

        self.logger.info("-------------Second Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info(self.failed_extract)

        self.logger.info("-------------Third Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info(self.failed_extract)

        end = time.time()
        self.logger.info("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        self.logger.info("Number of Data Base Enters = {}".format(self.no_of_db_entries))


if __name__ == '__main__':
    spider = YahooFinancial(datetime.datetime.today().date(),
                            targeted_population='STOCK+AARON',
                            batch=True,
                            loggerFileName=None)
    spider._extract_api('AAPL')
    # spider.run()
