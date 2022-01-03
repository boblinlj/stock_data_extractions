import pandas as pd
import numpy as np
import requests
from datetime import date
from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from sqlalchemy import create_engine
import random
import os
import time
import logging
from util.helper_functions import create_log
from util.request_website import FinvizParserPerPage


class Finviz:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    cnn = create_engine(
        f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
        pool_size=20,
        max_overflow=0)

    url_base = 'https://finviz.com/screener.ashx?v=152&ft=4'

    rename = {'Ticker': 'ticker',
              'Company': 'company_name',
              'Sector': 'sector',
              'Industry': 'industry',
              'Country': 'country',
              'Market Cap': 'market_cap',
              'P/E': 'pe',
              'Fwd P/E': 'fwd_pe',
              'PEG': 'peg',
              'P/S': 'ps',
              'P/B': 'pb',
              'P/C': 'pc',
              'P/FCF': 'pfcf',
              'Dividend': 'dividend',
              'Payout Ratio': 'payout_ratio',
              'EPS': 'eps',
              'EPS this Y': 'eps_this_y',
              'EPS next Y': 'eps_next_y',
              'EPS past 5Y': 'eps_past_5y',
              'EPS next 5Y': 'eps_next_5y',
              'Sales past 5Y': 'sales_past_5y',
              'EPS Q/Q': 'eps_q_q',
              'Sales Q/Q': 'sales_q_q',
              'Outstanding': 'outstanding',
              'Float': 'float_shares',
              'Insider Own': 'insider_own',
              'Insider Trans': 'insider_trans',
              'Inst Own': 'inst_own',
              'Inst Trans': 'inst_trans',
              'Float Short': 'float_short',
              'Short Ratio': 'short_ratio',
              'ROA': 'roa',
              'ROE': 'roe',
              'ROI': 'roi',
              'Curr R': 'curr_r',
              'Quick R': 'quick_r',
              'LTDebt/Eq': 'ltdebt_eq',
              'Debt/Eq': 'debt_eq',
              'Gross M': 'gross_m',
              'Oper M': 'oper_m',
              'Profit M': 'profit_m',
              'Perf Week': 'perf_week',
              'Perf Month': 'perf_month',
              'Perf Quart': 'perf_quart',
              'Perf Half': 'perf_half',
              'Perf Year': 'perf_year',
              'Perf YTD': 'perf_ytd',
              'Beta': 'beta',
              'ATR': 'atr',
              'Volatility W': 'volatility_w',
              'Volatility M': 'volatility_m',
              'SMA20': 'sma20',
              'SMA50': 'sma50',
              'SMA200': 'sma200',
              '50D High': '50d_high',
              '50D Low': '50d_low',
              '52W High': '52w_high',
              '52W Low': '52w_low',
              'RSI': 'rsi',
              'from Open': 'from_open',
              'Gap': 'gap',
              'Recom': 'recom',
              'Avg Volume': 'avg_volume',
              'Rel Volume': 'rel_volume',
              'Price': 'price',
              'Change': 'price_chg',
              'Volume': 'volume',
              'Earnings': 'earnings',
              'Target Price': 'target_price',
              'IPO Date': 'ipo_date'}

    no_of_requests = 0
    no_of_db_entries = 0

    def __init__(self, updated_dt, loggerFileName=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.logger = create_log(loggerName='Finviz', loggerFileName=self.loggerFileName)

    def _get_header(self):
        user_agent = random.choice(jcfg.UA_LIST)
        headers = {
            'user-agent': user_agent,
            'authority': 'uat5.investingchannel.com',
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'iframe',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'origin': 'https://finviz.com'
        }
        return headers

    def _get_session(self):
        session = requests.session()

        # session.proxies = {'http': 'socks5://{}:{}'.format(PROXY_URL, PROXY_PROT),
        #                    'https': 'socks5://{}:{}'.format(PROXY_URL, PROXY_PROT)}

        session.headers = self._get_header()

        return session

    def url_builder(self, Num):
        url_opt = '&c=' + ','.join(str(i) for i in range(1, 71))
        return self.url_base + f'&r={Num}' + url_opt

    def _get_response(self, url):
        self.no_of_requests += 1
        session = self._get_session()

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

    def _get_main_table(self, url):
        response = self._get_response(url)
        try:
            df = pd.read_html(response.text)[7]
        except Exception as e:
            self.logger.debug(e)
            return pd.DataFrame()

        if df.shape[1] == 70:
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df.replace(to_replace='-', value=np.NaN, inplace=True)
            return df
        else:
            return pd.DataFrame()

    @staticmethod
    def convert_str_to_num(str_v: str):
        if not (isinstance(str_v, str)):
            data = str_v
        elif 'K' in str_v:
            try:
                data = int(float(str_v.split('K')[0]) * 1000)
            except:
                data = str_v
        elif 'M' in str_v:
            try:
                data = int(float(str_v.split('M')[0]) * 1000000)
            except:
                data = str_v
        elif 'B' in str_v:
            try:
                data = int(float(str_v.split('B')[0]) * 1000000000)
            except:
                data = str_v
        elif 'T' in str_v:
            try:
                data = int(float(str_v.split('B')[0]) * 1000000000000)
            except:
                data = str_v
        elif '%' in str_v:
            try:
                data = float(str_v.split('%')[0]) / 100
            except:
                data = str_v
        elif ',' in str_v:
            try:
                data = str_v.replace(',', '')
            except:
                data = str_v
        elif '-' == str_v:
            data = None
        else:
            data = str_v

        return data

    def _add_database_information(self, df):
        df.rename(columns=self.rename, inplace=True)

        for column in df:
            df[column] = df[column].apply(self.convert_str_to_num)

        df['ipo_date'] = pd.to_datetime(df['ipo_date'], format='%m/%d/%Y')
        df['updated_dt'] = self.updated_dt

        return df

    def _enter_db(self, df, table):
        self.no_of_db_entries += 1
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=False, method='multi', chunksize=200)
            return True
        except Exception as e:
            self.logger.debug(e)
            return False

    def _number_of_stocks_in_finviz(self):
        ini_url = self.url_builder(1)

        response = self._get_response(ini_url)

        try:
            df = pd.read_html(response.text)[6]
        except Exception as e:
            self.logger.debug(e)
            return None

        if df.shape[0] == 1:
            return int(df[0][0].split(' ')[1])
        else:
            return None

    def _get_existing_entries(self, table, keys: list, stocks: list):
        elements = ','.join(x for x in keys)
        tickers = ','.join("'" + x + "'" for x in stocks)

        sql = f"""SELECT {elements} FROM {table} WHERE updated_dt = '{self.updated_dt}' AND ticker in ({tickers})"""

        try:
            return pd.read_sql(sql=sql, con=self.cnn)
        except Exception as e:
            self.logger.debug(e)
            return pd.DataFrame()

    def _check_existing_entries_financial(self, df_to_check, table):
        stock_lst = df_to_check.ticker.values

        df_existing_data = self._get_existing_entries(table, ['ticker', 'updated_dt'], stock_lst)
        df_existing_data.set_index('ticker', inplace=True)
        df_to_check.set_index('ticker', inplace=True)

        keep_or_drop = pd.concat([df_to_check, df_existing_data], axis=0).index.duplicated(keep=False)
        pd_after_ck = pd.concat([df_to_check, df_existing_data])[~keep_or_drop]
        pd_after_ck.reset_index(inplace=True)

        return pd_after_ck

    def _loop_all_tickers(self):

        no_of_stock = self._number_of_stocks_in_finviz()

        if no_of_stock is None:
            self.logger.debug("Failed to extract number of stocks, exit the program")
            exit()

        self.logger.info(f"There are {no_of_stock} stocks to be extracted")
        n = 1

        while n <= no_of_stock:
            # print(n)
            tmp = self._get_main_table(self.url_builder(n))
            tmp = self._add_database_information(tmp)

            no_of_stock_get_from_finviz = tmp.shape[0]
            n = n + no_of_stock_get_from_finviz

            finviz = self._check_existing_entries_financial(tmp, 'finviz_tickers')
            # somehow the function above impacts the tmp index
            tmp.reset_index(inplace=True)
            screener = self._check_existing_entries_financial(tmp, 'finviz_screener')

            # enter finviz
            if not finviz.empty:
                if self._enter_db(
                        finviz[['ticker', 'company_name', 'sector', 'industry', 'country', 'market_cap', 'pe', 'price',
                                'price_chg', 'updated_dt']], "finviz_tickers"):
                    self.logger.info('{} finviz_tickers data entered successfully'.format(n))
                else:
                    self.logger.info('{} finviz_tickers enter failed'.format(n))
            else:
                self.logger.info('{} finviz_tickers data already exist'.format(n))

            # enter finviz screener
            if not screener.empty:
                if self._enter_db(
                        screener[['ticker', 'market_cap', 'pe', 'fwd_pe', 'peg', 'ps', 'pb', 'pc', 'pfcf', 'dividend',
                                  'payout_ratio', 'eps', 'eps_this_y', 'eps_next_y', 'eps_past_5y', 'eps_next_5y',
                                  'sales_past_5y', 'eps_q_q', 'sales_q_q', 'outstanding', 'float_shares',
                                  'insider_own', 'insider_trans', 'inst_own', 'inst_trans', 'float_short',
                                  'short_ratio', 'roa', 'roe',
                                  'roi', 'curr_r', 'quick_r', 'ltdebt_eq', 'debt_eq', 'gross_m', 'oper_m', 'profit_m',
                                  'perf_week', 'perf_month', 'perf_quart', 'perf_half', 'perf_year', 'perf_ytd',
                                  'beta', 'atr', 'volatility_w', 'volatility_m', 'sma20', 'sma50', 'sma200',
                                  '50d_high', '50d_low', '52w_high', '52w_low', 'rsi', 'from_open', 'gap', 'recom',
                                  'avg_volume', 'rel_volume', 'price', 'price_chg', 'volume', 'earnings',
                                  'target_price', 'ipo_date', 'updated_dt']], "finviz_screener"):
                    self.logger.info('{} finviz_screener data entered successfully'.format(n))
                else:
                    self.logger.info('{} finviz_screener enter failed'.format(n))
            else:
                self.logger.info('{} finviz_screener data already exist'.format(n))

    def run(self):
        start = time.time()
        self._loop_all_tickers()
        end = time.time()

        self.logger.info("{} requests, took {} minutes".format(self.no_of_requests, round((end - start) / 60)))
        self.logger.info("Number of Data Base Enters = {}".format(self.no_of_db_entries))


if __name__ == '__main__':
    # test
    finviz = Finviz('2021-12-24', loggerFileName=None)
    finviz.run()
