from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
from sqlalchemy import create_engine
import datetime
import requests
import random
import time
import json
import pandas as pd
import os
import logging
from util.helper_functions import *
from util.parallel_process import *

pd.set_option('display.max_columns', None)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

today = datetime.date.today()

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler(
    os.path.join(jcfg.JOB_ROOT, 'logs', 'yahoo_financials_{}.log'.format(today.strftime("%b-%d-%Y"))))
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class YahooFinancial:
    BASE_URL = 'https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}'

    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_PASSWORD

    workers = jcfg.WORKER

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    df_for_elements = pd.read_csv(os.path.join(jcfg.JOB_ROOT, 'inputs', 'yahoo_financial_fundamental.csv'))

    all_elements = df_for_elements.data.values.tolist()
    annual_elements = df_for_elements[df_for_elements['freq'] == 'annual'].data.values.tolist()
    quarterly_elements = df_for_elements[df_for_elements['freq'] == 'quarterly'].data.values.tolist()
    ttm_elements = df_for_elements[df_for_elements['freq'] == 'TTM'].data.values.tolist()

    tdk = str(int(time.mktime(datetime.datetime.now().timetuple())))

    no_of_requests = 0
    no_of_db_entries = 0

    table_lookup = {'yahoo_quarterly_fundamental_t2': 'quarter',
                    'yahoo_annual_fundamental': 'annual',
                    'yahoo_trailing_fundamental': 'ttm'}

    failed_extract = []

    def __init__(self, updated_dt):
        self.updated_dt = updated_dt

    def _existing_dt(self):
        sql = """
                select distinct a.ticker, a.asOfDate, 'annual' as type
                from financial.yahoo_annual_fundamental a
                union
                select distinct a.ticker, a.asOfDate, 'quarter' as type
                from financial.yahoo_quarterly_fundamental a
                union
                select distinct a.ticker, a.asOfDate, 'ttm' as type
                from financial.yahoo_trailing_fundamental a
                order by ticker, type, asOfDate
                """
        self.ext_list_data = pd.read_sql(sql=sql, con=self.cnn)

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
            logger.debug(err2)
            return None
        except requests.exceptions.RequestException as err1:
            logger.debug(err1)
            return None

        return response

    def _url_builder_fundamentals(self):

        BASE_URL = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{stock}?symbol={stock}&type='
        URL_TAIL = '&merge=false&period1=493590046&period2=' + self.tdk
        url = ''

        for element in self.all_elements:
            url = url + element + '%2C'

        return BASE_URL + url + URL_TAIL

    def _get_json_data_from_html(self, stock):
        url = self._url_builder_fundamentals().format(stock=stock)

        response = self._get_response(url)

        if response is not None:
            try:
                data = json.loads(response.text)
                return data
            except Exception as e:
                logger.debug(e)
                return None
        else:
            return None

    def _enter_db(self, df, table):
        self.no_of_db_entries += 1
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=False, method='multi', chunksize=200)
            return True
        except Exception as e:
            logger.debug(e)
            return False

    def _extract_each_stock(self, stock):
        df_12m = pd.DataFrame()
        df_3m = pd.DataFrame()
        df_ttm = pd.DataFrame()

        trail = 1

        while trail <= 5:
            data = self._get_json_data_from_html(stock)
            if data is not None:
                break
            trail += 1

        if data is None:
            self.failed_extract.append(stock)
            return None

        for i in data['timeseries']['result']:
            col_name = i['meta']['type'][0]
            if col_name in self.annual_elements:
                df_12m[col_name] = self.process_data(col_name, i)
            elif col_name in self.quarterly_elements:
                df_3m[col_name] = self.process_data(col_name, i)
            elif col_name in self.ttm_elements:
                df_ttm[col_name] = self.process_data(col_name, i)
            else:
                pass

        df_3m = self._check_existing_entries_financial(df_3m, stock, 'yahoo_quarterly_fundamental_t2')
        if df_3m.empty:
            logger.info('Quarterly financial data already exist for stock = {}'.format(stock))
        else:
            df_3m = self._add_info_for_db(df_3m, stock)
            if self._enter_db(df_3m.reset_index(), 'yahoo_quarterly_fundamental_t2'):
                logger.info("Quarterly financial data is entered successfully for stock = {}".format(stock))
            else:
                logger.info("Quarterly financials database enter failed for stock = {}".format(stock))
                self.failed_extract.append(stock)

    @staticmethod
    def dedup_list(ls: list):
        return list(dict.fromkeys(ls))

    @staticmethod
    def process_data(column_name, data):
        if len(data) == 3:
            data_lst = []
            for n in data[data['meta']['type'][0]]:
                val = n['reportedValue']['raw']
                n.pop('reportedValue')
                n[column_name] = val
                data_lst.append(n)

            temp_ser = pd.DataFrame(data=data_lst).set_index('asOfDate').drop(['dataId', 'currencyCode', 'periodType'],
                                                                              axis=1)

            return temp_ser.squeeze('columns')
        else:
            return pd.Series(name=column_name, dtype='float64')

    def _add_info_for_db(self, db, stock):
        db['ticker'] = stock
        db['updated_dt'] = self.updated_dt

        return db

    def _get_stock_list(self):
        sql = """SELECT DISTINCT ticker 
                    FROM `finviz_tickers` 
                    where industry not like "REIT%" and industry <> "Exchange Traded Fund" 
                        and updated_dt = (
                            SELECT max(updated_dt) 
                            from finviz_tickers) 
                            order by volume DESC, market_cap desc"""

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def _get_stock_list_from_arron(self):
        sql = """SELECT DISTINCT yahoo_ticker 
                    FROM stock_list_for_cooble_stone """

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.yahoo_ticker.to_list()

    def _check_existing_entries_financial(self, df_to_check, stock, table):

        df_existing_data = self.ext_list_data[
            (self.ext_list_data['type'] == self.table_lookup[table]) & (self.ext_list_data['ticker'] == stock)]

        existing_asofdate = df_existing_data['asOfDate'].map(str).values

        df_after_check = df_to_check[~df_to_check.index.isin(existing_asofdate)]

        return df_after_check

    def run(self):
        start = time.time()
        self._existing_dt()

        logger.info("-------------First Extract Starts-------------")
        stocks = self._get_stock_list() + self._get_stock_list_from_arron()
        logger.info("{} Stocks to be extracted".format(len(stocks)))
        parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        logger.info("-------------Second Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        logger.info("-------------Third Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        end = time.time()
        logger.info("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        logger.info("Number of Data Base Enters = {}".format(self.no_of_db_entries))

    def run_batch(self):
        start = time.time()
        self._existing_dt()

        logger.info("-------------First Extract Starts-------------")
        stocks = self._get_stock_list() + self._get_stock_list_from_arron()
        logger.info("{} Stocks to be extracted".format(len(stocks)))
        non_parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        logger.info("-------------Second Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        non_parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        logger.info("-------------Third Extract Starts-------------")
        stocks = dedup_list(self.failed_extract)
        self.failed_extract = []
        non_parallel_process(stocks, self._extract_each_stock)
        logger.info(self.failed_extract)

        end = time.time()
        logger.info("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        logger.info("Number of Data Base Enters = {}".format(self.no_of_db_entries))

        print("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        print("Number of Data Base Enters = {}".format(self.no_of_db_entries))


if __name__ == '__main__':
    spider = YahooFinancial(datetime.datetime.today().date())
    spider._existing_dt()
    spider._extract_each_stock('AAPL')
