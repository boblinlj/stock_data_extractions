import sys
import time
import pandas as pd
import numpy as np
from util.helper_functions import dedup_list, create_log
import configs.job_configs as jcfg
import datetime
from util.parallel_process import parallel_process
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.get_stock_population import SetPopulation


class ExtractionError(Exception):
    pass


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

            if 'annual' in var_name:
                periodType = '12M'
            elif 'quarterly' in var_name:
                periodType = '3M'
            else:
                periodType = 'TTM'

            one_list = []

            if len(each_result) == 3:
                for data in each_result[var_name]:
                    one_list.append(
                        {"ticker": ticker,
                         var_name: data['reportedValue']['raw'],
                         'asOfDate': data['asOfDate'],
                         'currencyCode': data['currencyCode']}
                    )
            else:
                one_list.append(
                    {"ticker": ticker,
                     var_name: np.NaN,
                     'asOfDate': np.NaN,
                     'currencyCode': np.NaN}
                )

            temp_df = pd.DataFrame.from_records(data=one_list)
            temp_df['asOfDate'] = pd.to_datetime(temp_df['asOfDate'])
            temp_df.set_index(['ticker', 'asOfDate'], inplace=True)

            data_dic[periodType].append(temp_df)

        df_12m = pd.concat(data_dic['12M'], axis=1, sort=True).dropna(how='all')
        df_3m = pd.concat(data_dic['3M'], axis=1, sort=True).dropna(how='all')
        df_ttm = pd.concat(data_dic['TTM'], axis=1, sort=True).dropna(how='all')

        df_12m = df_12m.loc[:, ~df_12m.columns.duplicated()].copy()
        df_3m = df_3m.loc[:, ~df_3m.columns.duplicated()].copy()
        df_ttm = df_ttm.loc[:, ~df_ttm.columns.duplicated()].copy()

        return df_12m, df_3m, df_ttm


class YahooFinancial:
    workers = jcfg.WORKER
    BASE_URL = 'https://query1.finance.yahoo.com'
    df_for_elements = DatabaseManagement(sql="""SELECT type, freq, data
                                            FROM `yahoo_financial_statement_data_control`""").read_to_df()
    all_elements = df_for_elements.data.values.tolist()
    no_of_requests = 0
    no_of_db_entries = 0
    table_lookup = {'yahoo_quarterly_fundamental': 'quarter',
                    'yahoo_annual_fundamental': 'annual',
                    'yahoo_trailing_fundamental': 'ttm'}
    failed_extract = []

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None, use_tqdm=True):
        self.updated_dt = updated_dt
        self.targeted_population = targeted_pop
        self.loggerFileName = loggerFileName
        self.batch = batch
        self.logger = create_log(loggerName='YahooFinancialStatements', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0
        # number of database entries
        self.data_entries = 0

    def _existing_dt(self) -> None:
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

    def _url_builder_fundamentals(self) -> str:
        tdk = str(int(time.mktime(datetime.datetime.now().timetuple())))
        yahoo_fundamental_url = '/ws/fundamentals-timeseries/v1/finance/timeseries/{stock}?symbol={stock}&type='
        yahoo_fundamental_url_tail = '&merge=false&period1=493590046&period2=' + tdk
        elements = '%2C'.join(self.all_elements)

        return self.BASE_URL + yahoo_fundamental_url + elements + yahoo_fundamental_url_tail

    def _extract_api(self, stock) -> dict:
        url = self._url_builder_fundamentals().format(stock=stock)
        apiparse = YahooAPIParser(url=url)
        data = apiparse.parse()
        self.no_of_requests = self.no_of_requests + apiparse.no_requests
        if data is None:
            self.logger.debug(f'unable to get yahoo API data for stock={stock}')
        else:
            return data

    def _extract_each_stock(self, stock) -> None:
        self.logger.info(f"Processing {stock} for fundamental data")
        js = self._extract_api(stock)
        if js is None:
            self.failed_extract.append(stock)
            return None

        # extract data from YAHOO JSON
        df_12m, df_3m, df_ttm = ReadYahooFinancialData(js).parse()

        self._insert_to_db(df_12m, stock, 'yahoo_annual_fundamental')
        self._insert_to_db(df_3m, stock, 'yahoo_quarterly_fundamental')
        self._insert_to_db(df_ttm, stock, 'yahoo_trailing_fundamental')

    def _insert_to_db(self, df, stock, table) -> None:
        df_to_insert = df.copy()
        df_to_insert['updated_dt'] = self.updated_dt
        df_to_insert = self._check_existing_entries_financial(df_to_check=df_to_insert, stock=stock, table=table)
        try:
            DatabaseManagement(data_df=df_to_insert, table=table, insert_index=True).insert_db()
            self.data_entries += 1
            self.logger.info(f"{stock} data entered to {table} successfully")
        except DatabaseManagementError as e:
            self.logger.debug(f"Failed to insert data for stock={stock} as {e}")

    def _check_existing_entries_financial(self, df_to_check, stock, table) -> pd.DataFrame:
        df_existing_data = self.ext_list_data[
            (self.ext_list_data['type'] == self.table_lookup[table]) &
            (self.ext_list_data['ticker'] == stock)]
        df_existing_data.set_index(['ticker', 'asOfDate'], inplace=True)
        df_after_check = df_to_check[~df_to_check.index.isin(df_existing_data.index)]
        return df_after_check

    def run(self) -> None:
        start = time.time()
        self._existing_dt()

        stocks = SetPopulation(user_pop=self.targeted_population).setPop()
        self.no_of_stock = len(stocks)

        for _ in range(3):
            self.logger.info(f"{'-'*20}Start Extraction{'-'*20}")
            if self.batch:
                parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stocks, self._extract_each_stock, n_jobs=1)

            stocks = self.failed_extract
            self.failed_extract = []
            self.logger.info(f"{'-'*20}Extract Ends{'-'*20}")

        end = time.time()

        self.time_decay = end - start

    @property
    def get_failed_extracts(self):
        return len(self.failed_extract)


if __name__ == '__main__':
    spider = YahooFinancial(datetime.datetime.today().date() - datetime.timedelta(days=-3),
                            targeted_pop='YAHOO_STOCK_ALL',
                            batch=True,
                            loggerFileName=None)

    spider.run()
