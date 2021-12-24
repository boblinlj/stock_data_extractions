import requests
import pandas as pd
import json
import random
import datetime
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
from util.helper_functions import regulartime_to_unix
from util.helper_functions import unix_to_regulartime

class YahooPrice:
    YAHOO_API_URL_BASE = 'https://query1.finance.yahoo.com'

    CHART_API = '/v8/finance/chart/{ticker}' \
                '?symbol={ticker}&' \
                'period1={start}&' \
                'period2={end}&' \
                'interval={interval}&' \
                'includePrePost={prepost}&' \
                'events=div%2Csplit'

    def __init__(self, stock, start_dt, end_dt, interval='1d', includePrePost='false', logger=None):
        self.stock = stock
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.interval = interval
        self.prepost = includePrePost
        self.logger = logger

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
            if self.logger is None:
                print(err2)
            else:
                self.logger.degub(err2)
            return None
        except requests.exceptions.RequestException as err1:
            if self.logger is None:
                print(err1)
            else:
                self.logger.degub(err1)
            return None

        return response

    def get_each_stock_price_from_yahoo_chart(self):
        url = self.YAHOO_API_URL_BASE + self.CHART_API
        start = regulartime_to_unix(self.start_dt)
        end = regulartime_to_unix(self.end_dt)

        url = url.format(ticker=self.stock,
                         start=start,
                         end=end,
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
                output_df['timestamp'] = pd.to_datetime(output_df['timestamp'].apply(unix_to_regulartime),
                                                        format='%Y-%m-%d')
                output_df['ticker'] = self.stock
                return output_df

            except ValueError:
                return pd.DataFrame(
                    columns=['adjclose', 'close', 'high', 'low', 'open', 'timestamp', 'volume', 'ticker'])

        else:
            return pd.DataFrame(
                columns=['adjclose', 'close', 'high', 'low', 'open', 'timestamp', 'volume', 'ticker'])


if __name__ == '__main__':
    obj = YahooPrice('VET.TO', start_dt=datetime.date(2000, 1, 1), end_dt=datetime.date(2021, 12, 8))
    print(obj.get_each_stock_price_from_yahoo_chart())
