from configs import yahoo_configs as ycfg
from datetime import date
import json
import pandas as pd
import numpy as np
import time
from dataclasses import dataclass
from util.get_stock_population import SetPopulation
from util.helper_functions import create_log
from util.helper_functions import unix_to_regular_time
from util.helper_functions import dedup_list
from util.helper_functions import returnNotMatches
from util.parallel_process import *
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement, DatabaseManagementError


@dataclass
class ReadYahooStatsData:
    data: dict

    def parse(self) -> pd.DataFrame:

        # defaultKeyStatistics
        json_data = json.dumps(self.data['quoteSummary']['result'][0]['defaultKeyStatistics'])
        defaultKeyStatistics = pd.read_json(json_data).transpose()
        defaultKeyStatistics.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # financialData
        json_data = json.dumps(self.data['quoteSummary']['result'][0]['financialData'])
        financialData = pd.read_json(json_data).transpose()
        financialData.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # summaryDetail
        json_data = json.dumps(self.data['quoteSummary']['result'][0]['summaryDetail'])
        summaryDetail = pd.read_json(json_data).transpose()
        summaryDetail.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # price
        json_data = json.dumps(self.data['quoteSummary']['result'][0]['price'])
        price = pd.read_json(json_data).transpose()
        price.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        final_df = pd.concat([defaultKeyStatistics, financialData, summaryDetail, price])

        # remove the duplicated columns
        final_df = final_df.transpose()
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]

        for col in ycfg.YAHOO_STATS_COLUMNS:
            if col in final_df.columns:
                final_df[col] = np.where(final_df[col] == 'Infinity', np.nan, final_df[col])
            else:
                final_df[col] = np.nan
        try:
            final_df['sharesShortPreviousMonthDate'] = final_df['sharesShortPreviousMonthDate'].apply(
                unix_to_regular_time)
        except ValueError:
            final_df['sharesShortPreviousMonthDate'] = np.nan

        final_df['lastFiscalYearEnd'] = final_df['lastFiscalYearEnd'].apply(unix_to_regular_time)
        final_df['nextFiscalYearEnd'] = final_df['nextFiscalYearEnd'].apply(unix_to_regular_time)
        final_df['mostRecentQuarter'] = final_df['mostRecentQuarter'].apply(unix_to_regular_time)
        final_df.rename(columns={"symbol": "ticker"}, inplace=True)
        final_df.reset_index(drop=True, inplace=True)

        return final_df


class YahooStats:
    yahoo_module = ['defaultKeyStatistics', 'financialData', 'summaryDetail', 'price']
    BASE_URL = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{stock}?modules='+'%2C'.join(yahoo_module)
    workers = jcfg.WORKER
    failed_extract = []

    def __init__(self, updated_dt: date, targeted_pop: str, batch=False, loggerFileName=None, use_tqdm=True, test_size=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        self.test_size = test_size
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0

    def _get_stock_statistics(self, stock) -> pd.DataFrame:
        try:
            # parse the Yahoo API, it returns a JSON and a class variable of number of website calls
            apiparse = YahooAPIParser(url=self.BASE_URL.format(stock=stock))
            data = apiparse.parse()
            self.no_of_web_calls = self.no_of_web_calls + apiparse.no_requests
            out_df = ReadYahooStatsData(data).parse()
            out_df['lastDividendDate'] = unix_to_regular_time(out_df['lastDividendDate'])
            out_df['exDividendDate'] = unix_to_regular_time(out_df['exDividendDate'])
            out_df['lastSplitDate'] = unix_to_regular_time(out_df['lastSplitDate'])
            out_df['ticker'] = stock
            out_df['updated_dt'] = self.updated_dt
            return out_df

        except Exception as e:
            self.logger.error("Fail to extract stock = {}, error: {}".format(stock, e))
            return pd.DataFrame()

    def _extract_each_stock(self, stock) -> None:
        data_df = self._get_stock_statistics(stock)
        if data_df.empty:
            self.logger.debug('Fail to find {} data after {} trails'.format(stock, 5))
            self.failed_extract.append(stock)
        # enter yahoo fundamental table
        try:
            DatabaseManagement(data_df=data_df[ycfg.YAHOO_STATS_COLUMNS], table='yahoo_fundamental').insert_db()
            self.logger.info("yahoo_fundamental: Yahoo statistics data entered successfully for stock = {}".format(stock))
        except (DatabaseManagementError, KeyError) as e:
            self.logger.error(f"yahoo_fundamental: Yahoo statistics data entered failed for stock = {stock}, {e}")
            self.failed_extract.append(stock)

        return None

    def _existing_stock_list(self) -> pd.DataFrame:
        return DatabaseManagement(table='yahoo_fundamental',
                                  key='ticker',
                                  where=f"updated_dt = '{self.updated_dt}'").check_population()

    def _extract_population(self) -> list:
        stocks = SetPopulation(self.targeted_pop).setPop()
        stocks = dedup_list(stocks)
        stocks = returnNotMatches(stocks, self._existing_stock_list() + jcfg.BLOCK)[:]
        return stocks

    def run(self) -> None:
        start = time.time()

        stocks = self._extract_population()

        if self.test_size is not None:
            stocks = stocks[:self.test_size]

        self.no_of_stock = len(stocks)

        for _ in range(3):
            self.logger.info(f"{'-'*20}Start Extraction{'-'*20}")
            if self.batch:
                parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stocks, self._extract_each_stock, n_jobs=1)
            stocks = dedup_list(self.failed_extract)
            self.failed_extract = []
            self.logger.info(f"{'-'*20}Extract Ends{'-'*20}")

        end = time.time()
        self.time_decay = round((end - start) / 60)

    @property
    def get_failed_extracts(self):
        return len(self.failed_extract)


if __name__ == '__main__':
    spider = YahooStats(date(9999, 4, 20),
                        targeted_pop='YAHOO_STOCK_ALL',
                        batch=False,
                        loggerFileName=None,
                        use_tqdm=False)
    spider.run()
