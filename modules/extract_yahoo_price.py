import pandas as pd
from datetime import date
from util.helper_functions import regular_time_to_unix
from util.helper_functions import unix_to_regular_time
from util.helper_functions import create_log
from util.request_website import YahooAPIParser, WebParseError
from util.database_management import DatabaseManagement, DatabaseManagementError


class ReadYahooPrice:
    def __init__(self, js, details_ind=False):
        self.js = js
        self.details_ind = details_ind

    def price_only(self):

        data = self.js['chart']['result'][0]
        try:
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
            output_df.set_index(['timestamp'], inplace=True)

            return output_df
        except KeyError:
            return pd.DataFrame(columns=['adjclose', 'close', 'high', 'low', 'open', 'timestamp', 'volume'])

    def details(self):
        price_df = self.price_only()

        # form the dividends data
        try:
            data = self.js['chart']['result'][0]
            dividends_df = pd.DataFrame(data['events'].get('dividends'))
            if not (dividends_df.empty):
                dividends_df = dividends_df.transpose()
                dividends_df.reset_index(inplace=True)
                dividends_df['timestamp'] = pd.to_numeric(dividends_df['index'])
                dividends_df['timestamp'] = pd.to_datetime(dividends_df['timestamp'].apply(unix_to_regular_time),
                                                           format='%Y-%m-%d')
                dividends_df.drop(columns=['index', 'date'], axis=1, inplace=True)
                # dividends_df['ticker'] = self.stock
                dividends_df['lst_div_date'] = dividends_df['timestamp']
                dividends_df.set_index(['timestamp'], inplace=True)
                dividends_df.rename(columns={'amount': 'dividends'}, inplace=True)
                dividends_df.sort_index(inplace=True)
                dividends_df['t4q_dividends'] = dividends_df['dividends'].rolling(4).sum()
        except KeyError:
            dividends_df = pd.DataFrame(index=price_df.index, columns=['lst_div_date', 'dividends', 't4q_dividends'])
        # form the split data
        try:
            data = self.js['chart']['result'][0]
            splits_df = pd.DataFrame(data['events'].get('splits'))
            if not splits_df.empty:
                splits_df = splits_df.transpose()
                splits_df.reset_index(inplace=True)
                splits_df['timestamp'] = pd.to_numeric(splits_df['index'])
                splits_df['timestamp'] = pd.to_datetime(splits_df['timestamp'].apply(unix_to_regular_time),
                                                        format='%Y-%m-%d')
                splits_df.drop(columns=['index', 'date'], axis=1, inplace=True)
                # splits_df['ticker'] = self.stock
                splits_df['lst_split_date'] = splits_df['timestamp']
                splits_df.set_index(['timestamp'], inplace=True)
                splits_df.sort_index(inplace=True)
        except KeyError:
            splits_df = pd.DataFrame(index=price_df.index, columns=['lst_split_date', 'denominator', 'numerator', 'splitRatio'])

        output_df = pd.concat([price_df, dividends_df, splits_df], axis=1)
        output_df.fillna(method='ffill', inplace=True)

        return output_df

    def parse(self):
        if self.details_ind:
            return self.details()
        else:
            return self.price_only()


class YahooPrice:
    YAHOO_API_URL_BASE = 'https://query1.finance.yahoo.com'

    CHART_API = '/v8/finance/chart/{ticker}' \
                '?symbol={ticker}&' \
                'period1={start}&' \
                'period2={end}&' \
                'interval={interval}&' \
                'includePrePost={prepost}&' \
                'events=div%2Csplit'

    def __init__(self, stock, start_dt, end_dt=None, interval='1d', includePrePost='false', loggerFileName=None):
        self.stock = stock
        self.start_dt = regular_time_to_unix(start_dt)
        if end_dt is None:
            self.end_dt = 9999999999
        else:
            self.end_dt = regular_time_to_unix(end_dt)
        self.interval = interval
        self.includePrePost = includePrePost
        self.loggerFileName = loggerFileName
        self.latest_data_date = DatabaseManagement(table='price',
                                                   key='max(timestamp)',
                                                   where=f"ticker = '{self.stock}'"
                                                   ).check_population()[0]
        self.logger = create_log(loggerName='YahooPrice', loggerFileName=None)

    def get_basic_stock_price(self):
        url = self.YAHOO_API_URL_BASE + self.CHART_API
        url = url.format(ticker=self.stock,
                         start=self.start_dt,
                         end=self.end_dt,
                         interval=self.interval,
                         prepost=self.includePrePost)
        try:
            js = YahooAPIParser(url=url).parse()
            if js is None:
                self.logger.debug(f"{self.stock} does not have correct data from Yahoo")
            else:
                output = ReadYahooPrice(js, details_ind=False).parse()
                output['ticker'] = self.stock
                return output
        except WebParseError:
            self.logger.debug(f"{self.stock} does not have correct API from Yahoo")

    def get_detailed_stock_price(self):
        url = self.YAHOO_API_URL_BASE + self.CHART_API
        url = url.format(ticker=self.stock,
                         start=self.start_dt,
                         end=self.end_dt,
                         interval=self.interval,
                         prepost=self.includePrePost)
        try:
            js = YahooAPIParser(url=url).parse()
            if js is None:
                self.logger.debug(f"{self.stock} does not have correct data from Yahoo")
            else:
                output_df = ReadYahooPrice(js, details_ind=True).parse()
                output_df['ticker'] = self.stock
                if self.latest_data_date is None:
                    return output_df
                else:
                    return output_df.iloc[output_df.index > pd.to_datetime(self.latest_data_date)]
        except WebParseError:
            self.logger.debug(f"{self.stock} does not have correct API from Yahoo")


if __name__ == '__main__':
    obj = YahooPrice(stock='AAPL',
                     start_dt=date(2021, 1, 1),
                     end_dt=None,
                     interval='1d',
                     includePrePost='false',
                     loggerFileName=None)

    print(obj.get_detailed_stock_price())
