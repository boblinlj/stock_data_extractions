from configs import yahoo_configs as ycfg
from datetime import date
import json
import pandas as pd
import numpy as np
import time
from util.get_stock_population import StockPopulation
from util.helper_functions import create_log
from util.helper_functions import unix_to_regular_time
from util.helper_functions import dedup_list
from util.helper_functions import returnNotMatches
from util.parallel_process import *
from util.request_website import YahooWebParser, WebParseError
from util.database_management import DatabaseManagement, DatabaseManagementError


class ReadYahooStatsData:
    def __init__(self, data):
        self.data = data

    def parse(self):

        # defaultKeyStatistics
        json_data = json.dumps(
            self.data['context']['dispatcher']['stores']['QuoteSummaryStore']['defaultKeyStatistics'])
        defaultKeyStatistics = pd.read_json(json_data).transpose()
        defaultKeyStatistics.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # financialData
        json_data = json.dumps(self.data['context']['dispatcher']['stores']['QuoteSummaryStore']['financialData'])
        financialData = pd.read_json(json_data).transpose()
        financialData.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # summaryDetail
        json_data = json.dumps(self.data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryDetail'])
        summaryDetail = pd.read_json(json_data).transpose()
        summaryDetail.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

        # price
        json_data = json.dumps(self.data['context']['dispatcher']['stores']['QuoteSummaryStore']['price'])
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
    BASE_URL = 'https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}'

    workers = jcfg.WORKER

    stock_lst = StockPopulation()
    failed_extract = []

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)

    def _get_stock_statistics(self, stock):
        try:
            data = YahooWebParser(url=self.BASE_URL.format(stock=stock)).parse()
            out_df = ReadYahooStatsData(data).parse()
            out_df['updated_dt'] = self.updated_dt
            return out_df
        except Exception as e:
            self.logger.debug("Fail to extract stock = {}, error: {}".format(stock, e))
            return pd.DataFrame()

    def _extract_each_stock(self, stock):

        data_df = pd.DataFrame()
        trail = 1
        while trail <= 5:
            data_df = self._get_stock_statistics(stock)
            if not data_df.empty:
                self.logger.info('{} data found after {} trails'.format(stock, trail))
                break
            trail += 1

        if data_df.empty:
            self.logger.info('Fail to find {} data after {} trails'.format(stock, trail))
            self.failed_extract.append(stock)
            return stock

        # enter yahoo fundamental table
        try:
            DatabaseManagement(data_df=data_df.drop(ycfg.YAHOO_STATS_DROP_COLUMNS, axis=1, errors='ignore'),
                               table='yahoo_fundamental').insert_db()
            self.logger.info(
                "yahoo_fundamental: Yahoo statistics data entered successfully for stock = {}".format(stock))
        except DatabaseManagementError:
            self.logger.info("yahoo_fundamental: Yahoo statistics data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        # enter yahoo price table
        try:
            DatabaseManagement(data_df=data_df[ycfg.PRICE_TABLE_COLUMNS],
                               table='yahoo_price').insert_db()
            self.logger.info("yahoo_price: Yahoo price data entered successfully for stock = {}".format(stock))
        except DatabaseManagementError:
            self.logger.info("yahoo_price: Yahoo price data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        # enter yahoo consensus table
        try:
            DatabaseManagement(data_df=data_df[ycfg.YAHOO_CONSENSUS_COLUMNS],
                               table='yahoo_consensus_price').insert_db()
            self.logger.info(
                "yahoo_consensus_price: Yahoo consensus data entered successfully for stock = {}".format(stock))
        except DatabaseManagementError:
            self.logger.info("yahoo_consensus_price: Yahoo consensus data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        return None

    def run(self):
        start = time.time()

        self.logger.info("-------------First Extract Starts-------------")
        stocks = self.stock_lst.all_stock()

        stocks = dedup_list(stocks)
        existing_rec = DatabaseManagement(table='yahoo_fundamental',
                                          key='ticker',
                                          where=f"updated_dt = '{self.updated_dt}'"
                                          ).check_population()
        stocks = returnNotMatches(stocks, existing_rec + jcfg.BLOCK)[:]
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=False)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------First Extract Ends-------------")

        self.logger.info("-------------Second Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=False)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------Second Extract Ends-------------")

        self.logger.info("-------------Third Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=False)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------Third Extract Ends-------------")

        end = time.time()
        print("took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    spider = YahooStats(date(9999, 12, 31), batch=True, loggerFileName=None)
    spider.run()
