from configs import job_configs as jcfg
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from util.helper_functions import dedup_list, returnNotMatches
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement
from util.get_stock_population import SetPopulation
import pandas as pd
from datetime import date
import numpy as np
import time


class ReadYahooAnalysisData:
    def __init__(self, data):
        self.data = data

    def parse(self):
        period_lst = {'0q': 'thisQ',
                      '+1q': 'next1Q',
                      '0y': 'thisFY',
                      '+1y': 'Next1FY',
                      '+5y': 'Next5FY',
                      '-5y': 'Last5FY',
                      '-1q': 'Last1Q',
                      '-2q': 'Last2Q',
                      '-3q': 'Last3Q',
                      '-4q': 'Last4Q'
                      }

        analysis = self.data['quoteSummary']['result'][0]

        # process earnings forecast data
        earnings_trend = analysis['earningsTrend']['trend']

        period_df_lst = []
        for period_trend in earnings_trend:
            one_trend_dic = {
                'endDate': [period_trend['endDate']],
                'period': [period_trend['period']],
                'growth': [period_trend['growth'].get('raw')],
                'earningsEstimate_Avg': [period_trend['earningsEstimate'].get('avg', {}).get('raw', np.NAN)],
                'earningsEstimate_Low': [period_trend['earningsEstimate'].get('low', {}).get('raw', np.NAN)],
                'earningsEstimate_High': [period_trend['earningsEstimate'].get('high', {}).get('raw', np.NAN)],
                'earningsEstimate_yearAgoEps': [period_trend['earningsEstimate'].get('yearAgoEps', {}).get('raw', np.NAN)],
                'earningsEstimate_numberOfAnalysts': [period_trend['earningsEstimate'].get('numberOfAnalysts', {}).get('raw', np.NAN)],
                'earningsEstimate_growth': [period_trend['earningsEstimate'].get('growth', {}).get('raw', np.NAN)],

                'revenueEstimate_Avg': [period_trend['revenueEstimate'].get('avg', {}).get('raw', np.NAN)],
                'revenueEstimate_Low': [period_trend['revenueEstimate'].get('low', {}).get('raw', np.NAN)],
                'revenueEstimate_High': [period_trend['revenueEstimate'].get('high', {}).get('raw', np.NAN)],
                'revenueEstimate_yearAgoEps': [period_trend['revenueEstimate'].get('yearAgoEps', {}).get('raw', np.NAN)],
                'revenueEstimate_numberOfAnalysts': [period_trend['revenueEstimate'].get('numberOfAnalysts', {}).get('raw', np.NAN)],
                'revenueEstimate_growth': [period_trend['revenueEstimate'].get('growth', {}).get('raw', np.NAN)],

                'epsTrend_current': [period_trend['epsTrend'].get('current', {}).get('raw', np.NAN)],
                'epsTrend_7daysAgo': [period_trend['epsTrend'].get('7daysAgo', {}).get('raw', np.NAN)],
                'epsTrend_30daysAgo': [period_trend['epsTrend'].get('30daysAgo', {}).get('raw', np.NAN)],
                'epsTrend_60daysAgo': [period_trend['epsTrend'].get('60daysAgo', {}).get('raw', np.NAN)],
                'epsTrend_90daysAgo': [period_trend['epsTrend'].get('90daysAgo', {}).get('raw', np.NAN)],

                'epsRevisions_upLast7days': [period_trend['epsRevisions'].get('upLast7days', {}).get('raw', np.NAN)],
                'epsRevisions_upLast30days': [period_trend['epsRevisions'].get('upLast30days', {}).get('raw', np.NAN)],
                'epsRevisions_downLast30days': [period_trend['epsRevisions'].get('downLast30days', {}).get('raw', np.NAN)],
                'epsRevisions_downLast90days': [period_trend['epsRevisions'].get('downLast90days', {}).get('raw', np.NAN)],
            }
            # update column names
            temp_dic = {}
            for key, value in one_trend_dic.items():
                temp_dic[key + '_' + period_lst[period_trend['period']]] = one_trend_dic[key]
            temp_df = pd.DataFrame.from_records(temp_dic)
            period_df_lst.append(temp_df)
        # merge every estimates horizontally
        earnings_trend_df = pd.concat(period_df_lst, axis=1)

        # process earning historical
        earningsHistory = analysis['earningsHistory']['history']
        hist_list = []
        for period_hist in earningsHistory:
            hist_dic = {
                'epsActual': [period_hist['epsActual'].get('raw')],
                'epsDifference': [period_hist['epsDifference'].get('raw')],
                'epsEstimate': [period_hist['epsEstimate'].get('raw')],
                'period': [period_hist['period']],
                'quarter': [period_hist['quarter'].get('fmt')],
                'surprisePercent': [period_hist['surprisePercent'].get('raw')]
            }
            temp_dic = {}
            for key, value in hist_dic.items():
                temp_dic[key + '_' + period_lst[period_hist['period']]] = hist_dic[key]
            temp_df = pd.DataFrame.from_records(temp_dic)
            hist_list.append(temp_df)
        earnings_history = pd.concat(hist_list, axis=1)
        # combine final results - earnings historical data and earnings forecast data
        output_df = pd.concat([earnings_history, earnings_trend_df], axis=1)

        return output_df


class YahooAnalysis:
    workers = jcfg.WORKER
    url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earningsTrend%2CearningsHistory"
    failed_extract = []

    def __init__(self, updated_dt, targeted_pop, batch_run=True, loggerFileName=None, use_tqdm=True, test_size=None):
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch_run = batch_run
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='yahoo_analysis', loggerFileName=self.loggerFileName)
        self.existing_rec = DatabaseManagement(table='yahoo_consensus',
                                               key='ticker',
                                               where=f"updated_dt = '{self.updated_dt}'").check_population()
        self.use_tqdm = use_tqdm
        self.test_size = test_size
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0

    def _get_analysis_data(self, stock):
        try:
            apiparse = YahooAPIParser(url=self.url.format(ticker=stock), proxy=True)
            data = apiparse.parse()
            self.no_of_web_calls = self.no_of_web_calls + apiparse.no_requests
            out_df = ReadYahooAnalysisData(data).parse()
            out_df['ticker'] = stock
            return out_df
        except Exception as e:
            self.logger.debug(e)
            self.failed_extract.append(stock)
            return pd.DataFrame()

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        stock_df = self._get_analysis_data(stock)
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock} due to the dataframe is empty")
        else:
            stock_df['updated_dt'] = self.updated_dt
            try:
                DatabaseManagement(data_df=stock_df, table='yahoo_consensus').insert_db()
                self.logger.info(f"Success: Entered stock = {stock}")
            except Exception as e:
                self.logger.debug(f"Failed: Entering stock = {stock}, due to {e}")

    def run_job(self):
        start = time.time()
        stocks = SetPopulation(self.targeted_pop).setPop()

        stocks = dedup_list(stocks)
        stock_list = returnNotMatches(stocks, self.existing_rec + jcfg.BLOCK)[:]

        if self.test_size is not None:
            stock_list = stock_list[:self.test_size]

        self.no_of_stock = len(stocks)

        self.logger.info(f'There are {self.no_of_stock} stocks to be extracted')
        for _ in range(3):
            self.logger.info(f"{'-' * 20}Start Extraction{'-' * 20}")
            if self.batch_run:
                parallel_process(stock_list, self._run_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stock_list, self._run_each_stock, n_jobs=1)
            stock_list = dedup_list(self.failed_extract)
            self.failed_extract = []
            self.logger.info(f"{'-' * 20} Extract Ends{'-' * 20}")
        end = time.time()
        self.time_decay = round((end - start) / 60)

    @property
    def get_failed_extracts(self):
        return len(self.failed_extract)


if __name__ == '__main__':
    obj = YahooAnalysis(updated_dt=date.today(),
                        targeted_pop='YAHOO_STOCK_ALL',
                        batch_run=True,
                        loggerFileName=None)
    obj._get_analysis_data('AAPL')
