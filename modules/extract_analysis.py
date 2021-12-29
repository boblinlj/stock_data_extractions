import numpy as np
from util.request_website import GetWebsite
import pandas as pd
from util.helper_functions import create_log

pd.set_option('display.max_columns', None)


class YahooAnalysis:
    request = GetWebsite
    url = "https://finance.yahoo.com/quote/{ticker}/analysis?p={ticker}"

    def __init__(self, stock, loggerFileName):
        self.stock = stock
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='yahoo_analysis', loggerFileName=self.loggerFileName)

    def get_analysis_data(self):
        data = self.request(self.url.format(ticker=self.stock), True).get_json()
        if data is None:
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
                    temp_dic[key+'_'+period_lst[period_trend['period']]] = one_trend_dic[key]
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
            output_df['ticker'] = self.stock

            return output_df

        except (ValueError, TypeError, KeyError) as e:
            self.logger.debug(f"{self.stock} has ValueError, TypeError, or KeyError")
            return pd.DataFrame()


if __name__ == "__main__":
    obj = YahooAnalysis('ABCD')
    obj.get_analysis_data()
