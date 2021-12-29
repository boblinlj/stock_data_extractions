from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
from configs import yahoo_configs as ycfg
from sqlalchemy import create_engine
import datetime
import requests
import random
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
from util.get_stock_population import StockPopulation
import logging

import time
from util.helper_functions import create_log
from util.helper_functions import unix_to_regular_time
from util.helper_functions import dedup_list
from util.helper_functions import returnNotMatches
from util.parallel_process import *


class YahooStats:
    BASE_URL = 'https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}'

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
    no_of_requests = 0
    no_of_db_entries = 0

    failed_extract = []

    stock_lst = StockPopulation()

    def __init__(self, updated_dt, batch=False, loggerFileName=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.batch = batch
        self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)

    def _get_header(self):
        user_agent = random.choice(jcfg.UA_LIST)
        headers = {
            'user-agent': user_agent,
            'authority': 'ca.finance.yahoo.com',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'iframe',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site'
        }
        return headers

    def _get_response(self, url):
        self.no_of_requests += 1
        session = requests.session()
        session.proxies = {'http': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT),
                           'https': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT)}
        session.headers = self._get_header()

        try:
            response = session.get(url, allow_redirects=False)
        except requests.exceptions.ConnectTimeout:
            # try once more
            response = session.get(url, allow_redirects=False)
        except requests.exceptions.HTTPError as err2:
            self.logger.debug(err2)
            return None
        except requests.exceptions.RequestException as err1:
            self.logger.debug(err1)
            return None

        return response

    def _parse_html_for_json(self, response, stock):
        soup = BeautifulSoup(response.text, 'html.parser')
        pattern = re.compile(r'\s--\sData\s--\s')
        try:
            script_data = soup.find('script', text=pattern).contents[0]
        except AttributeError as err:
            self.logger.debug("Unable to find JSON for stock = {}".format(stock))
            return None

        start = script_data.find("context") - 2

        if start >= 0:
            json_data = json.loads(script_data[start:-12])
        else:
            json_data = None

        return json_data

    def _get_stock_statistics(self, stock):

        response = self._get_response(self.BASE_URL.format(stock=stock))

        if response is None:
            return pd.DataFrame()
        elif response.status_code == 200:
            data = self._parse_html_for_json(response, stock)
            if data is None:
                return pd.DataFrame()
        else:
            return pd.DataFrame()

        try:
            # defaultKeyStatistics
            json_data = json.dumps(data['context']['dispatcher']['stores']['QuoteSummaryStore']['defaultKeyStatistics'])
            defaultKeyStatistics = pd.read_json(json_data).transpose()
            defaultKeyStatistics.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

            # financialData
            json_data = json.dumps(data['context']['dispatcher']['stores']['QuoteSummaryStore']['financialData'])
            financialData = pd.read_json(json_data).transpose()
            financialData.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

            # summaryDetail
            json_data = json.dumps(data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryDetail'])
            summaryDetail = pd.read_json(json_data).transpose()
            summaryDetail.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')

            # price
            json_data = json.dumps(data['context']['dispatcher']['stores']['QuoteSummaryStore']['price'])
            price = pd.read_json(json_data).transpose()
            price.drop(['fmt', 'longFmt'], axis=1, inplace=True, errors='ignore')
        except Exception as e:
            self.logger.debug("Fail to extract stock = {}, error: {}".format(stock, e))
            return pd.DataFrame()

        final_df = pd.concat([defaultKeyStatistics, financialData, summaryDetail, price])
        final_df.rename(columns={'raw': stock}, inplace=True)

        # remove the duplicated columns
        final_df = final_df.transpose()
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]

        return final_df

    def _add_database_info_for_stock_statistics(self, df):

        df['updated_dt'] = self.updated_dt

        for col in ycfg.YAHOO_STATS_COLUMNS:
            if col in df.columns:
                df[col] = np.where(df[col] == 'Infinity', np.nan, df[col])
            else:
                df[col] = np.nan
        try:
            df['sharesShortPreviousMonthDate'] = df['sharesShortPreviousMonthDate'].apply(unix_to_regular_time)
        except ValueError:
            df['sharesShortPreviousMonthDate'] = np.nan

        df['lastFiscalYearEnd'] = df['lastFiscalYearEnd'].apply(unix_to_regular_time)
        df['nextFiscalYearEnd'] = df['nextFiscalYearEnd'].apply(unix_to_regular_time)
        df['mostRecentQuarter'] = df['mostRecentQuarter'].apply(unix_to_regular_time)
        df.rename(columns={"symbol": "ticker"}, inplace=True)
        # df.drop(YAHOO_STATS_DROP_COLUMNS, axis=1, inplace=True, errors='ignore')

        return df

    def _enter_db(self, df, table):
        self.no_of_db_entries += 1
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=False, method='multi', chunksize=200)
            return True
        except Exception as e:
            self.logger.debug(e)
            return False

    def _check_entries_stat(self):
        sql = f"""SELECT distinct ticker 
                  FROM yahoo_fundamental a 
                  WHERE a.updated_dt = '{self.updated_dt}' """

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def _extract_each_stock(self, stock):

        df_0 = pd.DataFrame()
        trail = 1
        while trail <= 5:
            df_0 = self._get_stock_statistics(stock)
            if not df_0.empty:
                self.logger.info('{} data found after {} trails'.format(stock, trail))
                break
            trail += 1

        if df_0.empty:
            self.logger.info('Fail to find {} data after {} trails'.format(stock, trail))
            self.failed_extract.append(stock)
            return stock

        df_1 = self._add_database_info_for_stock_statistics(df_0)

        # enter yahoo fundamental table
        if self._enter_db(df_1.drop(ycfg.YAHOO_STATS_DROP_COLUMNS, axis=1, errors='ignore'), 'yahoo_fundamental'):
            self.logger.info(
                "yahoo_fundamental: Yahoo statistics data entered successfully for stock = {}".format(stock))
        else:
            self.logger.info("yahoo_fundamental: Yahoo statistics data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        # enter yahoo price table
        if self._enter_db(df_1[ycfg.PRICE_TABLE_COLUMNS], 'yahoo_price'):
            self.logger.info("yahoo_price: Yahoo price data entered successfully for stock = {}".format(stock))
        else:
            self.logger.info("yahoo_price: Yahoo price data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        # enter yahoo consensus table
        if self._enter_db(df_1[ycfg.YAHOO_CONSENSUS_COLUMNS], 'yahoo_consensus_price'):
            self.logger.info("yahoo_consensus_price: Yahoo consensus data entered successfully for stock = {}".format(stock))
        else:
            self.logger.info("yahoo_consensus_price: Yahoo consensus data entered failed for stock = {}".format(stock))
            self.failed_extract.append(stock)

        return None

    def run(self):
        start = time.time()

        self.logger.info("-------------First Extract Starts-------------")
        stocks = self.stock_lst.get_stock_list() \
                 + self.stock_lst.get_stock_list_from_arron() \
                 + self.stock_lst.get_REIT_list() \
                 + self.stock_lst.get_ETF_list()

        stocks = dedup_list(stocks)
        existing_rec = self._check_entries_stat()
        stocks = returnNotMatches(stocks, existing_rec + jcfg.BLOCK)
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------First Extract Ends-------------")

        self.logger.info("-------------Second Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------Second Extract Ends-------------")

        self.logger.info("-------------Third Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        if self.batch:
            parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers)
        else:
            parallel_process(stocks, self._extract_each_stock, n_jobs=1)
        self.logger.info("-------------Third Extract Ends-------------")

        end = time.time()
        self.logger.info("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        self.logger.info("Number of Data Base Enters = {}".format(self.no_of_db_entries))


if __name__ == '__main__':
    spider = YahooStats(datetime.datetime.today().date(), batch=True, loggerFileName=None)
    spider.run()
