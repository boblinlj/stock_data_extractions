import numpy as np
import pandas as pd


def calculate_positive_EPS(input_df, period=12, TTM=True):
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
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_rdi(input_df):
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
        # print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_sgi(input_df):
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
        # print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_egNq(input_df, period: int):
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
        # print(f'{var_out} Error: not all inputs are available: {var1} or {var2}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])


def calculate_sgNq(input_df, period: int):
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
        # print(f'{var_out} Error: not all inputs are available = {var1} or {var2}')
        return pd.DataFrame(index=input_df.index, columns=[var_out])
