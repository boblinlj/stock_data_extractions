import pandas as pd
import numpy as np
from datetime import date
from configs import database_configs as dbcfg
from configs import job_configs as jcfg
from sqlalchemy import create_engine
from modules.extract_yahoo_price import YahooPrice

pd.set_option('mode.chained_assignment', None)
# pd.set_option('display.max_columns', None)


class CalculateFactors:
    market = '^GSPC'

    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    workers = jcfg.WORKER

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    sql_quarterly_data = """
                            select a.*
                            from yahoo_quarterly_fundamental a
                            where a.ticker='{}' and a.asOfDate between '{}' and '{}' 
                            order by a.asOfDate
                          """

    sql_date = """
                    select fulldate as asOfDate
                        ,dayofweek
                    from date_d
                    where fulldate>='{}' and fulldate<='{}'
                    order by fulldate
                """

    sql_last_entery = """
                        SELECT max(asOfDate) as asOfDate
                        FROM model_1_factors
                        where ticker='{}'
                    """

    sql_yahoo_fundamental = """
                                SELECT a.ticker
                                    , a.updated_dt as asOfDate
                                    , a.beta
                                    , a.currentPrice
                                    , a.forwardPE
                                    , a.forwardEps
                                    , a.trailingPE
                                    , a.trailingEps
                                    , a.ebitda
                                FROM yahoo_fundamental a 
                                where a.ticker='{}' and a.updated_dt >= '{}'
                                order by updated_dt
                            """

    sql_yahoo_consensus = """
                            select a.updated_dt as asOfDate,
                                    a.targetMedianPrice
                            from yahoo_consensus a
                            WHERE a.ticker='{}' and a.updated_dt between '{}' and '{}' 
                            order by updated_dt
                        """

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

    def __init__(self, stock, start_dt, updated_dt):

        # init the input
        self.start_dt = start_dt
        self.updated_dt = updated_dt  # assuming the updated_dt is the end_dt
        self.stock = stock
        # read price factor data
        self.price_f = YahooPrice(self.stock, start_dt, updated_dt).get_each_stock_price_from_yahoo_chart()
        self.price_f.rename(columns={'timestamp': 'asOfDate'}, inplace=True)
        self.price_f.drop(columns=['ticker'], axis=1, inplace=True)
        self.price_f.set_index('asOfDate', inplace=True)

        self.market_f = YahooPrice(self.market, start_dt, updated_dt).get_each_stock_price_from_yahoo_chart()
        self.market_f.rename(columns={'timestamp': 'asOfDate'}, inplace=True)
        self.market_f.drop(columns=['ticker'], axis=1, inplace=True)
        self.market_f.set_index('asOfDate', inplace=True)

        # read time dimension data
        self.time_d = pd.read_sql(sql=self.sql_date.format(self.start_dt, self.updated_dt), con=self.cnn)
        self.time_d['asOfDate'] = pd.to_datetime(self.time_d['asOfDate'], format='%Y-%m-%d', errors='ignore')
        self.time_d.set_index('asOfDate', inplace=True)
        # read quarterly fundamental data
        self.quarterly_data = pd.read_sql(sql=self.sql_quarterly_data.format(self.stock,
                                                                             self.start_dt,
                                                                             self.updated_dt),
                                          con=self.cnn)
        self.quarterly_data['asOfDate'] = pd.to_datetime(self.quarterly_data['asOfDate'], format='%Y-%m-%d',
                                                         errors='ignore')
        self.quarterly_data['reportDate'] = self.quarterly_data['asOfDate']
        self.quarterly_data.drop(columns=['data_id', 'updated_dt'], axis=1, inplace=True)
        self.quarterly_data.set_index('asOfDate', inplace=True)
        # the latest record in weekly_non_price_factors
        self.last_weekly_entry = pd.read_sql(sql=self.sql_last_entery.format(self.stock), con=self.cnn).iloc[0, 0]

        # read the Yahoo consensus data
        self.yahoo_consensus = pd.read_sql(sql=self.sql_yahoo_consensus.format(self.stock,
                                                                               self.start_dt,
                                                                               self.updated_dt),
                                           con=self.cnn)
        self.yahoo_consensus.fillna(method='ffill', inplace=True)
        self.yahoo_consensus['asOfDate'] = pd.to_datetime(self.yahoo_consensus['asOfDate'],
                                                          format='%Y-%m-%d',
                                                          errors='ignore')
        self.yahoo_consensus.set_index('asOfDate', inplace=True)

    def _add_rolling_sum(self, input_df, input_cols: list, number_of_periods=4):
        # add every 4 quarter data, this function should only be applied to CF and IS items
        cols = input_cols
        out_cols = []
        stg_df = input_df[cols].copy(deep=True)
        stg_df.sort_index(inplace=True)

        for col in input_cols:
            out_var = 't' + str(number_of_periods) + 'q_' + col
            out_cols.append(out_var)
            if col in input_df.columns:
                # eg: t4q_quarterlyFreeCashFlow
                stg_df[out_var] = stg_df[col].rolling(number_of_periods, min_periods=number_of_periods).sum()
            else:
                print(f'_add_rolling_sum Error: not all inputs are available = {col}')
                stg_df[out_var] = np.nan

        return stg_df[out_cols]

    def _calculate_change_rate(self, input_df, input_cols: list, number_of_periods=4):
        # calculate the rate of changes for the trailing 12-month data
        cols = ['t4q_' + col for col in input_cols]
        out_cols = []
        stg_df = input_df[cols].copy(deep=True)
        stg_df.sort_index(inplace=True)

        for col in input_cols:
            tgt_col = 't4q_' + col
            out_var = str(number_of_periods) + 'q_chg_' + col
            out_cols.append(out_var)
            if tgt_col in input_df.columns:
                # eg. 4q_lag_quarterlyFreeCashFlow, this interim variable will be dropped later
                stg_df[str(number_of_periods) + 'q_lag_' + col] = input_df[tgt_col].shift(number_of_periods)
                # eg. 4q_chg_quarterlyFreeCashFlow
                stg_df[out_var] = input_df[tgt_col] / abs(stg_df[str(number_of_periods) + 'q_lag_' + col]) - 1
            else:
                print(f'_calculate_change_rate Error: not all inputs are available = {col}')
                stg_df[str(number_of_periods) + 'q_lag_' + col] = np.nan
                stg_df[out_var] = np.nan

        return stg_df[out_cols]

    def _calculate_rolling_median(self, input_df, input_cols: list, number_of_periods=8):
        # calculate the median change rate of the trailing 12 month data
        cols = ['4q_chg_' + col for col in input_cols]
        out_cols = []
        stg_df = input_df[cols].copy(deep=True)
        stg_df.sort_index(inplace=True)

        for col in input_cols:
            tgt_col = '4q_chg_' + col
            out_var = str(number_of_periods) + 'q_median_growth_' + col
            out_cols.append(out_var)
            if tgt_col in input_df.columns:
                # eg. 4q_median_growth_quarterlyFreeCashFlow
                stg_df[out_var] = input_df[tgt_col].rolling(number_of_periods, min_periods=number_of_periods).median()
            else:
                print(f'_calculate_rolling_median Error: not all inputs are available = {col}')
                stg_df[out_var] = np.nan

        return stg_df[out_cols]

    def _calculate_rolling_stdev(self, input_df, input_cols: list, number_of_periods=8):
        # calculate the standard deviation change rate of the trailing 12 month data
        cols = ['4q_chg_' + col for col in input_cols]
        out_cols = []
        stg_df = input_df[cols].copy(deep=True)
        stg_df.sort_index(inplace=True)

        for col in input_cols:
            tgt_col = '4q_chg_' + col
            out_var = str(number_of_periods) + 'q_growth_stdev_' + col
            out_cols.append(out_var)
            if tgt_col in input_df.columns:
                # eg. 4q_growth_stdev_quarterlyFreeCashFlow
                stg_df[out_var] = input_df[tgt_col].rolling(number_of_periods, min_periods=number_of_periods).std()
            else:
                print(f'_calculate_rolling_stdev Error: not all inputs are available = {col}')
                stg_df[out_var] = np.nan

        return stg_df[out_cols]

    def _calculate_positive_EPS(self, input_df, period=12, TTM=True):
        # calculate the number of positive EPS in the past n periods
        # if TTM is True, the program will use the t4q_ values
        stg_df = input_df.sort_index(ascending=True).copy()
        if TTM:
            var = 'quarterlyDilutedEPS'
            var_out = 'posqeps_' + str(period) + 'qcount'
        else:
            var = 't4q_' + 'quarterlyDilutedEPS'
            var_out = 'posttmeg_' + str(period) + 'qcount'

        if var in stg_df.columns:
            stg_df['positive_ind'] = np.where(stg_df[var] > 0, 1, 0)
            stg_df[var_out] = stg_df['positive_ind'].rolling(period, min_periods=period).sum()
            stg_df.drop(columns=['positive_ind'], axis=1, inplace=True)

            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'_calculate_positive_EPS Error: not all inputs are available = {var}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_rdi(self, input_df):
        # calculate RDI as TTM R&D to TTM revenue
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlyResearchAndDevelopment'
        var2 = 't4q_' + 'quarterlyTotalRevenue'
        var_out = 'rdi'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = input_df[[var1, var2]].groupby(input_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_sgi(self, input_df):
        # calculate TTM SG&A to TTM revenue
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlySellingGeneralAndAdministration'
        var2 = 't4q_' + 'quarterlyTotalRevenue'
        var_out = 'sgi'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            output_df = stg_df.loc[stg_df[var_out].notnull()][[var_out]]
            return output_df
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_qsm(self, input_df, shift_periods=4):
        # Quarterly Sales Momentum = TTM sales / TTM sales a quarter ago - QSM
        stg_df = input_df.sort_index(ascending=True).copy()
        var = 't4q_' + 'quarterlyTotalRevenue'
        var_out = f'qsm_{shift_periods}q'

        if var in stg_df.columns:
            stg_df = stg_df[[var]].groupby(stg_df.index).first()
            stg_df[f'{var}_lag_{shift_periods}q'] = stg_df[var].shift(periods=shift_periods)
            stg_df[var_out] = stg_df[var] / stg_df[f'{var}_lag_{shift_periods}q']

            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]

        else:
            print(f'{var_out} Error: not all inputs are available = {var}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_fcfta(self, input_df):
        # TTM free cash flow to total asset - FCFTA
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlyFreeCashFlow'
        var2 = 'quarterlyTotalAssets'
        var_out = 'fcfta'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            output_df = stg_df.loc[stg_df[var_out].notnull()][[var_out]]
            return output_df
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_capacq(self, input_df):
        # capital acquisition ratio - CAPACQ
        # (TTM cash flow - TTM cash dividend) / TTM revenue
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlyOperatingCashFlow'
        var2 = 't4q_' + 'quarterlyCashDividendsPaid'
        var3 = 't4q_' + 'quarterlyTotalRevenue'
        var_out = 'capacq'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns) and (var3 in stg_df.columns):
            stg_df = stg_df[[var1, var2, var3]].groupby(stg_df.index).first()
            stg_df[var_out] = (stg_df[var1] - stg_df[var2]) / stg_df[var3]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2} or {var3}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_roe(self, input_df):
        # return on equity
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlyNetIncome'
        var2 = 'quarterlyStockholdersEquity'
        var_out = 'roe'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_roa(self, input_df):
        # return on asset
        stg_df = input_df.sort_index(ascending=True).copy()
        var1 = 't4q_' + 'quarterlyNetIncome'
        var2 = 'quarterlyTotalAssets'
        var_out = 'roa'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_egNq(self, input_df, period: int):
        # Median earnings growth over the past N quarters, adjusted by earnings growth volatility
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = str(period) + 'q_median_growth_quarterlyNetIncome'
        var2 = str(period) + 'q_growth_stdev_quarterlyNetIncome'
        var_out = 'eg' + str(period) + 'q'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available: {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_sgNq(self, input_df, period: int):
        # Median sales growth over the past N quarters, adjusted by sales growth volatility
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = str(period) + 'q_median_growth_quarterlyTotalRevenue'
        var2 = str(period) + 'q_growth_stdev_quarterlyTotalRevenue'
        var_out = 'sg' + str(period) + 'q'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_fcfyld(self, input_df):
        # Free cash flow yield = TTM FCF/Mkt_Cap - FCFYLD
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = 't4q_' + 'quarterlyFreeCashFlow'
        var2 = 'marketCap'
        var_out = 'fcfyld'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_eyldtrl(self, input_df):
        # Trailing earnings yield = TTM Net Income / Market Cap - EYLDTRL
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = 't4q_quarterlyNetIncome'
        var2 = 'marketCap'

        var_out = 'eyldtrl'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] / stg_df[var2]
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_eyldfwd(self, input_df, lookfwd_period_y: int = 3):
        # estimated forward earnings yield = trailing earnings yield * (1+median quarterly growth)^n - i.e. eyldfwd_12q
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = 'eyldtrl'
        var2 = str(lookfwd_period_y * 4) + 'q_median_growth_quarterlyNetIncome'

        var_out = 'eyld_fwd' + str(lookfwd_period_y) + 'yr'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df = stg_df[[var1, var2]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1] * ((1 + stg_df[var2]) ** (4 * lookfwd_period_y))
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_eyldavg(self, input_df, looking_proward_q: int = 16):
        stg_df = input_df.sort_index(ascending=True).copy()

        var1 = 'eyldtrl'
        var_out = f'eyld_{looking_proward_q}qavg'

        if var1 in stg_df.columns:
            stg_df = stg_df[[var1]].groupby(stg_df.index).first()
            stg_df[var_out] = stg_df[var1].rolling(looking_proward_q, min_periods=looking_proward_q).mean()
            return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _resample(self, input_df, freqeuncy='M'):
        output_df = input_df.copy()
        output_df_m = output_df.resample(freqeuncy).last()
        return output_df_m

    def _calculate_return(self, input_df, lag: int = 1, skip: int = 0, frequency='M'):

        if frequency in ['M', 'W', 'Y']:
            stg_df = self._resample(input_df, frequency)
        else:
            stg_df = input_df.sort_index(ascending=True).copy()

        var1 = 'adjclose'
        var_out = f'pch{lag}x{skip}{frequency}'.lower()

        stg_df[var_out] = stg_df[var1].shift(skip) / stg_df[var1].shift(lag + skip) - 1

        return stg_df[var_out]

    def _calculate_beta(self, stock_df, market_df, frequency='M', length: int = 60):
        stock_df_2 = self._calculate_return(stock_df, lag=1, skip=0, frequency=frequency)
        stock_df_2.name = f'stock_return_{frequency}'

        market_df_2 = self._calculate_return(market_df, lag=1, skip=0, frequency=frequency)
        market_df_2.name = f'market_return_{frequency}'

        output_df = pd.concat([stock_df_2, market_df_2], axis=1)

        output_df['cov'] = output_df[f'stock_return_{frequency}'].rolling(length).cov(
            output_df[f'market_return_{frequency}'])
        output_df['var'] = output_df[f'market_return_{frequency}'].rolling(length).var()
        output_df[f'beta_{length}{frequency}'.lower()] = output_df['cov'] / output_df['var']

        output_df.drop(columns=['cov', 'var', f'stock_return_{frequency}', f'market_return_{frequency}'],
                       axis=1,
                       inplace=True)

        return output_df.groupby(output_df.index).first()

    def _calculate_rdvol(self, input_df):
        stg_df = input_df.groupby(input_df.index).first().copy()
        var1 = 'volume'
        var2 = 'adjclose'
        var_out = 'rdvol'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df[var_out] = stg_df[var1] * stg_df[var2]
            return stg_df[var_out]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_max_drawdown(self, input_df, trading_days=252):
        stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
        var1 = 'high'
        var2 = 'adjclose'
        var_out = f'pcghi{trading_days}d'

        if (var1 in stg_df.columns) and (var2 in stg_df.columns):
            stg_df[f'high_{trading_days}d'] = stg_df[var1].rolling(trading_days).max()
            stg_df[f'pcghi{trading_days}d'] = stg_df[var2] / stg_df[f'high_{trading_days}d'] - 1
            return stg_df[var_out]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_rp(self, input_df, period, frequency='D'):
        stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
        var1 = 'adjclose'
        var_out = f'rp{period}{frequency}'

        if var1 in stg_df.columns:
            stg_df['average_price'] = stg_df[var1].rolling(period, min_periods=period).mean()
            stg_df[var_out] = stg_df[var1] / stg_df['average_price']
            return stg_df[var_out]
        else:
            print(f'{var_out} Error: not all inputs are available = {var1}')
            return pd.DataFrame(index=input_df.index, columns=[var_out])

    def _calculate_volatitliy(self, input_df, period, frequency='D'):
        stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
        if frequency in ['W', 'M', 'Y']:
            stg_df = self._resample(stg_df, frequency)
        if frequency == 'D':
            pass
        else:
            print('inviolate frequency, only accept D, W, M, Y')

        var_out = f'volatility{period}{frequency}'

        re = self._calculate_return(stg_df, lag=1, skip=0, frequency=frequency).to_frame()
        re[var_out] = re.rolling(period).std()
        return re[var_out]

    def _calculate_ram(self, stock_df, market_df, beta, lags: list, skips: list = [0], frequency='D', keep_beta=False):

        beta_df = self._calculate_beta(stock_df, market_df, beta[-1], int(beta.split(beta[-1])[0]))

        stock_return_dfs = []
        mk_return_dfs = []

        for lag in lags:
            for skip in skips:
                stock_df_tmp = self._calculate_return(stock_df, lag, skip, frequency='D')
                stock_return_dfs.append(stock_df_tmp)
                market_df_tmp = self._calculate_return(market_df, lag, skip, frequency='D')
                market_df_tmp.name = f'pch{lag}x{skip}d_mk'
                mk_return_dfs.append(market_df_tmp)

        comb_re = pd.concat(stock_return_dfs + mk_return_dfs, axis=1)
        comb_re = comb_re.groupby(comb_re.index).first()
        comb_re['key2'] = comb_re.index.year.astype(str) + '-' + comb_re.index.month.astype(str)
        beta_df['key2'] = beta_df.index.year.astype(str) + '-' + beta_df.index.month.astype(str)
        comb_re.reset_index(inplace=True)
        beta_df.reset_index(inplace=True)
        beta_df.drop(columns=['asOfDate'], axis=1, inplace=True)
        final_df = comb_re.merge(right=beta_df, how='left', on='key2')
        final_df.set_index('asOfDate', inplace=True)
        final_df.drop('key2', axis=1, inplace=True)

        # final_df[f'beta_{beta}'] = final_df[f'beta_{beta}'].fillna(method='ffill')
        final_df.fillna(method='ffill', inplace=True)

        for lag in lags:
            for skip in skips:
                final_df[f'ram{lag}x{skip}{frequency}'.lower()] = final_df[f'pch{lag}x{skip}{frequency}'.lower()] - \
                                                                  final_df[f'pch{lag}x{skip}{frequency}_mk'.lower()] * \
                                                                  final_df[f'beta_{beta}'.lower()]

                final_df.drop(columns=[f'pch{lag}x{skip}{frequency}'.lower(),
                                       f'pch{lag}x{skip}{frequency}_mk'.lower()
                                       ],
                              axis=1,
                              inplace=True)
        if not keep_beta:
            final_df.drop(columns=[f'beta_{beta}'.lower()], axis=1, inplace=True)

        return final_df

    def calculate_tparev(self, input_df, lag, frequency='D'):
        tmp = input_df.sort_index()
        var1 = 'targetMedianPrice'
        output = f'tparv{lag}{frequency}'.lower()

        if var1 in tmp.columns:
            tmp[var1 + '_lag'] = tmp[var1].shift(lag)
            tmp[output] = tmp[var1] / tmp[var1 + '_lag']
            tmp.drop(columns=[var1, var1 + '_lag'], axis=1, inplace=True)
            tmp.dropna(inplace=True)
            return tmp
        else:
            print(f'calculate_tparev Error: not all inputs are available = {var1}')
            return pd.DataFrame(index=tmp.index, columns=[output])

    def run_pipeline(self):

        if self.price_f.empty:
            return pd.DataFrame()
        elif self.quarterly_data.empty:
            return pd.DataFrame()

        # *************************************STEP ONE*************************************************************
        # start the basic calculations to prepare the inputs for factors
        df1 = self._add_rolling_sum(self.quarterly_data, self.trailing_factors_list, 4)
        df2 = self._calculate_change_rate(df1, self.trailing_factors_list, 4)
        df3 = self._calculate_rolling_median(df2, self.trailing_factors_list, 8)
        df4 = self._calculate_rolling_median(df2, self.trailing_factors_list, 12)
        df5 = self._calculate_rolling_stdev(df2, self.trailing_factors_list, 8)
        df6 = self._calculate_rolling_stdev(df2, self.trailing_factors_list, 12)

        data_frames = [self.quarterly_data, df1, df2, df3, df4, df5, df6]

        df_from_step1 = pd.concat(data_frames, axis=1)
        df_from_step1 = df_from_step1.groupby(df_from_step1.index).first()
        # *************************************END: STEP ONE********************************************************

        # ****************************************STEP TWO**********************************************************
        df_with_time_df = pd.concat([self.price_f, self.time_d, df_from_step1], axis=1)

        # calculate factors that do not need to use price data
        rdi = self._calculate_rdi(df_from_step1)
        sgi = self._calculate_sgi(df_from_step1)
        qsm = self._calculate_qsm(df_from_step1, 4)
        fcfta = self._calculate_fcfta(df_from_step1)
        capacq = self._calculate_capacq(df_from_step1)
        roe = self._calculate_roe(df_from_step1)
        roa = self._calculate_roa(df_from_step1)
        eg8g = self._calculate_egNq(df_from_step1, 8)
        sg12g = self._calculate_sgNq(df_from_step1, 12)
        posttmeg_12qcount = self._calculate_positive_EPS(df_from_step1, period=12, TTM=True)
        tparev21d = self.calculate_tparev(self.yahoo_consensus, lag=21, frequency='D')
        tparev63d = self.calculate_tparev(self.yahoo_consensus, lag=63, frequency='D')
        tparev126d = self.calculate_tparev(self.yahoo_consensus, lag=126, frequency='D')

        # calculate price factors
        ram1 = self._calculate_ram(self.price_f, self.market_f, beta='60M', lags=[126, 189, 252], skips=[21],
                                   frequency='D', keep_beta=False)
        ram2 = self._calculate_ram(self.price_f, self.market_f, beta='60M', lags=[21], skips=[0], frequency='D',
                                   keep_beta=True)
        pcghi12m = self._calculate_max_drawdown(self.price_f, 252)
        pch63d = self._calculate_return(self.price_f, lag=63, skip=0, frequency='D')
        rp10d = self._calculate_rp(self.price_f, period=10, frequency='D')
        volatility60d = self._calculate_volatitliy(self.price_f, period=60, frequency='D')
        volatility12m = self._calculate_volatitliy(self.price_f, period=252, frequency='D')

        data_frames2 = [df_with_time_df, rdi, sgi, qsm, fcfta, capacq, roe, roa, eg8g, sg12g, posttmeg_12qcount,
                        tparev21d, tparev63d, tparev126d, ram1, ram2, pcghi12m, pch63d, rp10d, volatility60d,
                        volatility12m]

        df_final = pd.concat(data_frames2, axis=1)

        df_final.fillna(method='ffill', inplace=True)

        # ****************************************END: STEP TWO*****************************************************

        # ****************************************STEP TREE*********************************************************
        df_final['marketCap'] = df_final['close'] * df_final['quarterlyBasicAverageShares']

        fcfyld = self._calculate_fcfyld(df_final)
        df_final = pd.concat([df_final, fcfyld], axis=1)

        eyldtrl = self._calculate_eyldtrl(df_final)
        df_final = pd.concat([df_final, eyldtrl], axis=1)

        eyldfwd = self._calculate_eyldfwd(df_final, 3)
        df_final = pd.concat([df_final, eyldfwd], axis=1)

        eyld16qavg = self._calculate_eyldavg(df_final, 16)
        df_final = pd.concat([df_final, eyld16qavg], axis=1)

        df_final['updated_dt'] = self.updated_dt

        df_final = df_final.loc[df_final['dayofweek'] == 6]
        # only select ones that are not in database

        # df_final = df_final[df_final.index > pd.to_datetime(self.last_weekly_entry)]

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
    obj = CalculateFactors('USCR', date(2010, 1, 1), date.today())
    df = obj.run_pipeline()
    print(df.tail())
