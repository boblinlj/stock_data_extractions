import requests
import pandas as pd
import json
import random
import datetime
from sqlalchemy import create_engine
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
from configs import database_configs as dbcfg
from util.helper_functions import regular_time_to_unix
from util.helper_functions import unix_to_regular_time
from util.helper_functions import create_log


class YahooPrice:
    database_user = dbcfg.MYSQL_USER
    database_ip = dbcfg.MYSQL_HOST
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    YAHOO_API_URL_BASE = 'https://query1.finance.yahoo.com'

    CHART_API = '/v8/finance/chart/{ticker}' \
                '?symbol={ticker}&' \
                'period1={start}&' \
                'period2={end}&' \
                'interval={interval}&' \
                'includePrePost={prepost}&' \
                'events=div%2Csplit'

    sql_max_date = "SELECT max(timestamp) FROM financial.price where ticker = '{}'"
    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    def __init__(self, stock, start_dt, end_dt, interval='1d', includePrePost='false', loggerFileName=None):
        self.stock = stock
        self.start_dt = regular_time_to_unix(start_dt)
        self.end_dt = regular_time_to_unix(end_dt)
        # self.end_dt = 9999999999
        self.interval = interval
        self.prepost = includePrePost
        self.loggerFileName = loggerFileName
        self.latest_data_date = pd.read_sql(con=self.cnn, sql=self.sql_max_date.format(self.stock)).iloc[0, 0]

        if self.loggerFileName is not None:
            self.logger = create_log(loggerName='YahooPrice', loggerFileName=self.loggerFileName)
        else:
            self.logger = create_log(loggerName='YahooPrice', loggerFileName=None)

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
            self.logger.degub(err2)
            return None
        except requests.exceptions.RequestException as err1:
            self.logger.degub(err1)
            return None

        return response

    def get_basic_stock_price(self):
        url = self.YAHOO_API_URL_BASE + self.CHART_API

        url = url.format(ticker=self.stock,
                         start=self.start_dt,
                         end=self.end_dt,
                         interval=self.interval,
                         prepost=self.prepost)

        response = self._get_response(url)

        if response.status_code == 200:
            js = json.loads(response.text)
            try:
                data = js['chart']['result'][0]
                output = {'timestamp': data['timestamp'],
                          'high': data['indicators']['quote'][0]['high'],
                          'close': data['indicators']['quote'][0]['close'],
                          'open': data['indicators']['quote'][0]['open'],
                          'low': data['indicators']['quote'][0]['low'],
                          'volume': data['indicators']['quote'][0]['volume'],
                          'adjclose': data['indicators']['adjclose'][0]['adjclose']}
                output_df = pd.DataFrame.from_records(output)
                output_df['timestamp'] = pd.to_datetime(output_df['timestamp'].apply(unix_to_regular_time),
                                                        format='%Y-%m-%d')
                output_df['ticker'] = self.stock
                return output_df
            except ValueError:
                self.logger.debug(f"{self.stock} value error, variables cannot be found in Yahoo API")
                return pd.DataFrame(
                    columns=['adjclose', 'close', 'high', 'low', 'open', 'timestamp', 'volume', 'ticker'])
        else:
            self.logger.debug(f"{self.stock} response error {response.status_code}")
            return pd.DataFrame(
                columns=['adjclose', 'close', 'high', 'low', 'open', 'timestamp', 'volume', 'ticker'])

    def get_detailed_stock_price(self):
        url = self.YAHOO_API_URL_BASE + self.CHART_API
        url = url.format(ticker=self.stock,
                         start=self.start_dt,
                         end=self.end_dt,
                         interval=self.interval,
                         prepost=self.prepost)

        response = self._get_response(url)
        # self.logger.info(url)
        if response.status_code == 200:
            js = json.loads(response.text)
            try:
                data = js['chart']['result'][0]
                # form the pricing data
                price = {'timestamp': data['timestamp'],
                         'high': data['indicators']['quote'][0]['high'],
                         'close': data['indicators']['quote'][0]['close'],
                         'open': data['indicators']['quote'][0]['open'],
                         'low': data['indicators']['quote'][0]['low'],
                         'volume': data['indicators']['quote'][0]['volume'],
                         'adjclose': data['indicators']['adjclose'][0]['adjclose']}
                price_df = pd.DataFrame.from_records(price)
                price_df['timestamp'] = pd.to_datetime(price_df['timestamp'].apply(unix_to_regular_time), format='%Y-%m-%d')
                price_df['ticker'] = self.stock
                price_df.set_index(['timestamp'], inplace=True)
            except ValueError:
                self.logger.debug(f"{self.stock} value error, variables cannot be found in Yahoo API")
                return pd.DataFrame()
            # form the dividends data
            try:
                dividends_df = pd.DataFrame(data['events'].get('dividends'))
                if not (dividends_df.empty):
                    dividends_df = dividends_df.transpose()
                    dividends_df.reset_index(inplace=True)
                    dividends_df['timestamp'] = pd.to_numeric(dividends_df['index'])
                    dividends_df['timestamp'] = pd.to_datetime(dividends_df['timestamp'].apply(unix_to_regular_time),
                                                               format='%Y-%m-%d')
                    dividends_df.drop(columns=['index', 'date'], axis=1, inplace=True)
                    dividends_df['ticker'] = self.stock
                    dividends_df['lst_div_date'] = dividends_df['timestamp']
                    dividends_df.set_index(['timestamp'], inplace=True)
                    dividends_df.rename(columns={'amount': 'dividends'}, inplace=True)
                    dividends_df.sort_index(inplace=True)
                    dividends_df['t4q_dividends'] = dividends_df['dividends'].rolling(4).sum()
            except KeyError:
                dividends_df = pd.DataFrame(index=price_df.index, columns=['lst_div_date', 'dividends', 't4q_dividends'])
            # form the split data
            try:
                splits_df = pd.DataFrame(data['events'].get('splits'))
                if not splits_df.empty:
                    splits_df = splits_df.transpose()
                    splits_df.reset_index(inplace=True)
                    splits_df['timestamp'] = pd.to_numeric(splits_df['index'])
                    splits_df['timestamp'] = pd.to_datetime(splits_df['timestamp'].apply(unix_to_regular_time),
                                                            format='%Y-%m-%d')
                    splits_df.drop(columns=['index', 'date'], axis=1, inplace=True)
                    splits_df['ticker'] = self.stock
                    splits_df['lst_split_date'] = splits_df['timestamp']
                    splits_df.set_index(['timestamp'], inplace=True)
                    splits_df.sort_index(inplace=True)
            except KeyError:
                splits_df = pd.DataFrame(index=price_df.index, columns=['lst_split_date', 'denominator', 'numerator', 'splitRatio'])

            output_df = pd.concat([price_df, dividends_df, splits_df], axis=1)
            output_df.fillna(method='ffill', inplace=True)

            return output_df.iloc[output_df.index > pd.to_datetime(self.latest_data_date)]
            # return output_df
        else:
            self.logger.debug(f"{self.stock} response error {response.status_code}")
            return pd.DataFrame()


if __name__ == '__main__':
    obj = YahooPrice('CREE', start_dt=datetime.date(2000, 1, 1), end_dt=datetime.date(2021, 12, 25))
    print(obj.get_detailed_stock_price())
