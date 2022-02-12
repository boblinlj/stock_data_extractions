import pandas as pd
import numpy as np
from datetime import date
from configs import job_configs as jcfg
from modules.extract_yahoo_price import YahooPrice
from util.helper_functions import create_log
from util.database_management import DatabaseManagement
# factor calculation models
from factors.factor_rolling_calc import *
from factors.value_factors import *
from factors.growth_factors import *
from factors.pmo_factors import *

pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_columns', None)


class FactorCalculationError(Exception):
    pass


class CalculateFactors:
    market = '^GSPC'  # use sp500 as the market index

    workers = jcfg.WORKER

    trailing_factors_list = [
        'quarterlyFreeCashFlow',
        'quarterlyCashDividendsPaid',
        'quarterlyOperatingCashFlow',
        'quarterlyTotalRevenue',
        'quarterlyNetIncome',
        'quarterlyResearchAndDevelopment',
        'quarterlySellingGeneralAndAdministration',
        'quarterlyBasicEPS',
        'quarterlyDilutedEPS'
    ]

    failed_extract = []

    def __init__(self, stock, start_dt, updated_dt, loggerFileName=None):

        self.start_dt = start_dt
        self.updated_dt = updated_dt  # assuming the updated_dt is the end_dt
        self.stock = stock
        # read price factor data
        self.price_f = YahooPrice(self.stock, start_dt, updated_dt, disable_log=True).get_basic_stock_price()
        if not self.price_f.empty:
            self.price_f.index.names = ['asOfDate']
            self.price_f.drop(columns=['ticker'], axis=1, inplace=True)

        self.market_f = YahooPrice(self.market, start_dt, updated_dt, disable_log=True).get_basic_stock_price()
        if not self.market_f.empty:
            self.market_f.drop(columns=['ticker'], axis=1, inplace=True)
            self.market_f.index.names = ['asOfDate']

        # read time dimension data
        self.time_d = DatabaseManagement(table='date_d',
                                         key='fulldate as asOfDate ,dayofweek',
                                         where=f"fulldate>='{self.start_dt}' and fulldate<='{self.updated_dt}'").get_record()
        self.time_d['asOfDate'] = pd.to_datetime(self.time_d['asOfDate'], format='%Y-%m-%d', errors='ignore')
        self.time_d.set_index('asOfDate', inplace=True)

        # read quarterly fundamental data
        self.quarterly_data = DatabaseManagement(key='*',
                                                 table='yahoo_quarterly_fundamental',
                                                 where=f"ticker='{self.stock}' and asOfDate between '{self.start_dt}' and '{self.updated_dt}' ").get_record()
        self.quarterly_data['asOfDate'] = pd.to_datetime(self.quarterly_data['asOfDate'], format='%Y-%m-%d', errors='ignore')
        self.quarterly_data['reportDate'] = self.quarterly_data['asOfDate']
        self.quarterly_data.drop(columns=['data_id', 'updated_dt'], axis=1, inplace=True)
        self.quarterly_data.set_index('asOfDate', inplace=True)

        # the latest record in weekly_non_price_factors
        self.last_weekly_entry = DatabaseManagement(table='model_1_factors',
                                                    key="max(asOfDate)",
                                                    where=f"ticker='{self.stock}'").check_population()[0]

        # read the Yahoo consensus data
        self.yahoo_consensus = DatabaseManagement(table='yahoo_consensus_price',
                                                  key='updated_dt as asOfDate, targetMedianPrice',
                                                  where=f"ticker='{self.stock}' and updated_dt between '{self.start_dt}' and '{self.updated_dt}'").get_record()
        self.yahoo_consensus.fillna(method='ffill', inplace=True)
        self.yahoo_consensus['asOfDate'] = pd.to_datetime(self.yahoo_consensus['asOfDate'], format='%Y-%m-%d', errors='ignore')
        self.yahoo_consensus.set_index('asOfDate', inplace=True)
        # create logger information
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='factors', loggerFileName=self.loggerFileName)

    def run_pipeline(self):

        if self.price_f.empty or self.price_f.empty:
            return pd.DataFrame()
        elif self.quarterly_data.empty:
            return pd.DataFrame()

        # *************************************STEP ONE*************************************************************
        # start the basic calculations to prepare the inputs for factors
        df1 = add_rolling_sum(self.quarterly_data, self.trailing_factors_list, 4)
        df2 = calculate_change_rate(df1, self.trailing_factors_list, 4)
        df3 = calculate_rolling_median(df2, self.trailing_factors_list, 8)
        df4 = calculate_rolling_median(df2, self.trailing_factors_list, 12)
        df5 = calculate_rolling_stdev(df2, self.trailing_factors_list, 8)
        df6 = calculate_rolling_stdev(df2, self.trailing_factors_list, 12)

        data_frames = [self.quarterly_data, df1, df2, df3, df4, df5, df6]

        df_from_step1 = pd.concat(data_frames, axis=1)
        df_from_step1 = df_from_step1.groupby(df_from_step1.index).first()
        # *************************************END: STEP ONE********************************************************

        # ****************************************STEP TWO**********************************************************
        df_with_time_df = pd.concat([self.price_f, self.time_d, df_from_step1], axis=1)

        # calculate factors that do not need to use price data
        rdi = calculate_rdi(df_from_step1)
        sgi = calculate_sgi(df_from_step1)
        qsm = calculate_qsm(df_from_step1, 4)
        fcfta = calculate_fcfta(df_from_step1)
        capacq = calculate_capacq(df_from_step1)
        roe = calculate_roe(df_from_step1)
        roa = calculate_roa(df_from_step1)
        eg8g = calculate_egNq(df_from_step1, 8)
        sg12g = calculate_sgNq(df_from_step1, 12)
        posttmeg_12qcount = calculate_positive_EPS(df_from_step1, period=12, TTM=True)
        tparev21d = calculate_tparev(self.yahoo_consensus, lag=21, frequency='D')
        tparev63d = calculate_tparev(self.yahoo_consensus, lag=63, frequency='D')
        tparev126d = calculate_tparev(self.yahoo_consensus, lag=126, frequency='D')

        # calculate price factors
        ram1 = calculate_ram(self.price_f, self.market_f, beta='60M', lags=[126, 189, 252], skips=[21],
                                   frequency='D', keep_beta=False)
        ram2 = calculate_ram(self.price_f, self.market_f, beta='60M', lags=[21], skips=[0], frequency='D',
                                   keep_beta=True)

        pcghi12m = calculate_max_drawdown(self.price_f, 252)
        pch63d = calculate_return(self.price_f, lag=63, skip=0, frequency='D')
        rp10d = calculate_rp(self.price_f, period=10, frequency='D')
        volatility60d = calculate_volatility(self.price_f, period=60, frequency='D')
        volatility12m = calculate_volatility(self.price_f, period=252, frequency='D')

        data_frames2 = [df_with_time_df, rdi, sgi, qsm, fcfta, capacq, roe, roa, eg8g, sg12g, posttmeg_12qcount,
                        tparev21d, tparev63d, tparev126d, ram1, ram2, pcghi12m, pch63d, rp10d, volatility60d,
                        volatility12m]

        df_final = pd.concat(data_frames2, axis=1)

        df_final.fillna(method='ffill', inplace=True)
        # ****************************************END: STEP TWO*****************************************************

        # ****************************************STEP TREE*********************************************************
        df_final['marketCap'] = df_final['close'] * df_final['quarterlyBasicAverageShares']

        fcfyld = calculate_fcfyld(df_final)
        df_final = pd.concat([df_final, fcfyld], axis=1)

        eyldtrl = calculate_eyldtrl(df_final)
        df_final = pd.concat([df_final, eyldtrl], axis=1)

        eyldfwd = calculate_eyldfwd(df_final, 3)
        df_final = pd.concat([df_final, eyldfwd], axis=1)

        eyld16qavg = calculate_eyldavg(df_final, 16)
        df_final = pd.concat([df_final, eyld16qavg], axis=1)

        df_final['updated_dt'] = self.updated_dt

        df_final = df_final.loc[df_final['dayofweek'] == 6]
        # only select ones that are not in database

        df_final = df_final[df_final.index > pd.to_datetime(self.last_weekly_entry)]

        df_final.drop(columns=['dayofweek'], inplace=True)

        col_2_delete = ['adjclose', 'high', 'low', 'open', 'volume']
        for col in self.quarterly_data.columns:
            if col not in ['ticker', 'asOfDate', 'reportDate'] and col in df_final.columns:
                col_2_delete.append(col)

        df_final.drop(columns=col_2_delete, axis=1, inplace=True)
        df_final.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_final.dropna(subset=['ticker', 'reportDate'], axis=0, inplace=True)

        return df_final


if __name__ == '__main__':
    loggerFileName = f"daily_yahoo_price_{date.today().strftime('%Y%m%d')}.log"

    obj = CalculateFactors('AMD', date(2010, 1, 1), date.today(),
                           loggerFileName=None)
    print(obj.last_weekly_entry)
    df = obj.run_pipeline()
    print(df)