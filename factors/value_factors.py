import numpy as np
import pandas as pd


def calculate_fcfta(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_capacq(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_roe(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_roa(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])

def calculate_fcfyld(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_eyldtrl(input_df):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_eyldfwd(input_df, lookfwd_period_y: int = 3):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_eyldavg(input_df, looking_proward_q: int = 16):
    stg_df = input_df.sort_index(ascending=True).copy()

    var1 = 'eyldtrl'
    var_out = f'eyld_{looking_proward_q}qavg'

    if var1 in stg_df.columns:
        stg_df = stg_df[[var1]].groupby(stg_df.index).first()
        stg_df[var_out] = stg_df[var1].rolling(looking_proward_q, min_periods=looking_proward_q).mean()
        return stg_df.loc[stg_df[var_out].notnull()][[var_out]]
    else:
        return pd.DataFrame(index=input_df.index, columns=[var_out])

