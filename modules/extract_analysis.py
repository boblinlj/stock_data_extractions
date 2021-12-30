from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from sqlalchemy import create_engine
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from util.create_output_sqls import write_insert_db
from util.gcp_functions import upload_to_bucket
from util.get_stock_population import StockPopulation
from util.helper_functions import dedup_list, returnNotMatches
from util.request_website import GetWebsite
import os
import pandas as pd
import time
from datetime import date
import numpy as np


class YahooAnalysis:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER
    request = GetWebsite
    url = "https://finance.yahoo.com/quote/{ticker}/analysis?p={ticker}"

    cnn = create_engine(
        f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
        pool_size=20,
        max_overflow=0)

    stock_lst = StockPopulation()

    no_of_db_entries = 0
    failed_extraction = []

    def __init__(self, updated_dt, upload_to_gcp=False, batch_run=True, loggerFileName=None):
        # init the input
        self.updated_dt = updated_dt
        self.batch_run = batch_run
        self.upload_to_gcp = upload_to_gcp
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='yahoo_analysis', loggerFileName=self.loggerFileName)

    def _check_entries_stat(self):
        sql = f"""
                SELECT distinct ticker 
                FROM yahoo_consensus a 
                WHERE a.updated_dt = '{self.updated_dt}'
                """
        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def _get_analysis_data(self, stock):
        data = self.request(self.url.format(ticker=stock), True).get_json()

        if data is None:
            self.logger.info(f'{stock} does not have Yahoo analysis data')
            return pd.DataFrame()

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
        try:
            analysis = data['context']['dispatcher']['stores']['QuoteSummaryStore']
            # process forecasts
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
                    'earningsEstimate_yearAgoEps': [
                        period_trend['earningsEstimate'].get('yearAgoEps', {}).get('raw', np.NAN)],
                    'earningsEstimate_numberOfAnalysts': [
                        period_trend['earningsEstimate'].get('numberOfAnalysts', {}).get('raw', np.NAN)],
                    'earningsEstimate_growth': [period_trend['earningsEstimate'].get('growth', {}).get('raw', np.NAN)],

                    'revenueEstimate_Avg': [period_trend['revenueEstimate'].get('avg', {}).get('raw', np.NAN)],
                    'revenueEstimate_Low': [period_trend['revenueEstimate'].get('low', {}).get('raw', np.NAN)],
                    'revenueEstimate_High': [period_trend['revenueEstimate'].get('high', {}).get('raw', np.NAN)],
                    'revenueEstimate_yearAgoEps': [
                        period_trend['revenueEstimate'].get('yearAgoEps', {}).get('raw', np.NAN)],
                    'revenueEstimate_numberOfAnalysts': [
                        period_trend['revenueEstimate'].get('numberOfAnalysts', {}).get('raw', np.NAN)],
                    'revenueEstimate_growth': [period_trend['revenueEstimate'].get('growth', {}).get('raw', np.NAN)],

                    'epsTrend_current': [period_trend['epsTrend'].get('current', {}).get('raw', np.NAN)],
                    'epsTrend_7daysAgo': [period_trend['epsTrend'].get('7daysAgo', {}).get('raw', np.NAN)],
                    'epsTrend_30daysAgo': [period_trend['epsTrend'].get('30daysAgo', {}).get('raw', np.NAN)],
                    'epsTrend_60daysAgo': [period_trend['epsTrend'].get('60daysAgo', {}).get('raw', np.NAN)],
                    'epsTrend_90daysAgo': [period_trend['epsTrend'].get('90daysAgo', {}).get('raw', np.NAN)],

                    'epsRevisions_upLast7days': [
                        period_trend['epsRevisions'].get('upLast7days', {}).get('raw', np.NAN)],
                    'epsRevisions_upLast30days': [
                        period_trend['epsRevisions'].get('upLast30days', {}).get('raw', np.NAN)],
                    'epsRevisions_downLast30days': [
                        period_trend['epsRevisions'].get('downLast30days', {}).get('raw', np.NAN)],
                    'epsRevisions_downLast90days': [
                        period_trend['epsRevisions'].get('downLast90days', {}).get('raw', np.NAN)],
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

            output_df = pd.concat([earnings_history, earnings_trend_df], axis=1)
            output_df['ticker'] = stock

            return output_df

        except (ValueError, TypeError, KeyError) as e:
            self.logger.debug(f"{stock} has ValueError, TypeError, or KeyError")
            return pd.DataFrame()

    def _run_each_stock(self, stock):
        self.logger.info(f"Start Processing stock = {stock}")
        stock_df = self._get_analysis_data(stock)
        if stock_df.empty:
            self.logger.debug(f"Failed:Processing stock = {stock} due to the dataframe is empty")
        else:
            stock_df['updated_dt'] = self.updated_dt
            if self._enter_db(stock_df, 'yahoo_consensus'):
                self.logger.info(f"Success: Entered stock = {stock}")
            else:
                self.logger.debug(f"Failed: Entering stock = {stock}")

    def _enter_db(self, df, table):
        try:
            df.to_sql(name=table, con=self.cnn, if_exists='append', index=False, method='multi', chunksize=200)
            return True
        except Exception as e:
            self.logger.debug(e)
            return False

    def run_job(self):
        start = time.time()

        stocks = self.stock_lst.get_stock_list() \
                 + self.stock_lst.get_stock_list_from_arron() \
                 + self.stock_lst.get_REIT_list() \
                 + self.stock_lst.get_ETF_list()

        stocks = dedup_list(stocks)
        existing_rec = self._check_entries_stat()
        stock_list = returnNotMatches(stocks, existing_rec + jcfg.BLOCK)

        self.logger.info(f'There are {len(stock_list)} stocks to be extracted')
        if self.batch_run:
            parallel_process(stock_list, self._run_each_stock, n_jobs=self.workers)
        else:
            parallel_process(stock_list, self._run_each_stock, n_jobs=1)


if __name__ == '__main__':
    loggerFileName = f"weekly_yahoo_consensus_{date.today().strftime('%Y%m%d')}.log"

    obj = YahooAnalysis(updated_dt=date(2021, 12, 29),
                        batch_run=True,
                        loggerFileName=loggerFileName)
    obj.run_job()
