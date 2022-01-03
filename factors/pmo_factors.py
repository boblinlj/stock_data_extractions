import pandas as pd
import numpy as np


def calculate_qsm(input_df, shift_periods=4):
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


def resample(input_df, freqeuncy='M'):
    output_df = input_df.copy()
    output_df_m = output_df.resample(freqeuncy).last()
    return output_df_m


def calculate_return(input_df, lag: int = 1, skip: int = 0, frequency='M'):

    if frequency in ['M', 'W', 'Y']:
        stg_df = resample(input_df, frequency)
    else:
        stg_df = input_df.sort_index(ascending=True).copy()

    var1 = 'adjclose'
    var_out = f'pch{lag}x{skip}{frequency}'.lower()

    stg_df[var_out] = stg_df[var1].shift(skip) / stg_df[var1].shift(lag + skip) - 1

    return stg_df[var_out]


def calculate_beta(stock_df, market_df, frequency='M', length: int = 60):
    stock_df_2 = calculate_return(stock_df, lag=1, skip=0, frequency=frequency)
    stock_df_2.name = f'stock_return_{frequency}'

    market_df_2 = calculate_return(market_df, lag=1, skip=0, frequency=frequency)
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


def calculate_rdvol(input_df):
    stg_df = input_df.groupby(input_df.index).first().copy()
    var1 = 'volume'
    var2 = 'adjclose'
    var_out = 'rdvol'

    if (var1 in stg_df.columns) and (var2 in stg_df.columns):
        stg_df[var_out] = stg_df[var1] * stg_df[var2]
        return stg_df[var_out]
    else:
        # print(f'{var_out} Error: not all inputs are available = {var1}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_max_drawdown(input_df, trading_days=252):
    stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
    var1 = 'high'
    var2 = 'adjclose'
    var_out = f'pcghi{trading_days}d'

    if (var1 in stg_df.columns) and (var2 in stg_df.columns):
        stg_df[f'high_{trading_days}d'] = stg_df[var1].rolling(trading_days).max()
        stg_df[f'pcghi{trading_days}d'] = stg_df[var2] / stg_df[f'high_{trading_days}d'] - 1
        return stg_df[var_out]
    else:
        # print(f'{var_out} Error: not all inputs are available = {var1}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_rp(input_df, period, frequency='D'):
    stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
    var1 = 'adjclose'
    var_out = f'rp{period}{frequency}'

    if var1 in stg_df.columns:
        stg_df['average_price'] = stg_df[var1].rolling(period, min_periods=period).mean()
        stg_df[var_out] = stg_df[var1] / stg_df['average_price']
        return stg_df[var_out]
    else:
        # print(f'{var_out} Error: not all inputs are available = {var1}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_volatility(input_df, period, frequency='D'):
    stg_df = input_df.sort_index(ascending=True).groupby(input_df.index).first().copy()
    if frequency in ['W', 'M', 'Y']:
        stg_df = resample(stg_df, frequency)
    if frequency == 'D':
        pass
    else:
        print('inviolate frequency, only accept D, W, M, Y')

    var_out = f'volatility{period}{frequency}'

    re = calculate_return(stg_df, lag=1, skip=0, frequency=frequency).to_frame()
    re[var_out] = re.rolling(period).std()
    return re[var_out]

def calculate_ram(stock_df, market_df, beta, lags: list, skips: list = [0], frequency='D', keep_beta=False):

    beta_df = calculate_beta(stock_df, market_df, beta[-1], int(beta.split(beta[-1])[0]))

    stock_return_dfs = []
    mk_return_dfs = []

    for lag in lags:
        for skip in skips:
            stock_df_tmp = calculate_return(stock_df, lag, skip, frequency='D')
            stock_return_dfs.append(stock_df_tmp)
            market_df_tmp = calculate_return(market_df, lag, skip, frequency='D')
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


def calculate_tparev(input_df, lag, frequency='D'):
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
        # print(f'calculate_tparev Error: not all inputs are available = {var1}')
        return pd.DataFrame(index=tmp.index, columns=[output])

