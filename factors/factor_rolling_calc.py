import numpy as np


class FactorCalculationError(Exception):
    pass

def add_rolling_sum(input_df,
                    input_cols: list,
                    number_of_periods=4):
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
            # raise FactorCalculationError(f'_add_rolling_sum Error: not all inputs are available = {col}')
            stg_df[out_var] = np.nan

    return stg_df[out_cols]


def calculate_change_rate(input_df,
                          input_cols: list,
                          number_of_periods=4):
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
            # print(f'_calculate_change_rate Error: not all inputs are available = {col}')
            # stg_df[str(number_of_periods) + 'q_lag_' + col] = np.nan
            stg_df[out_var] = np.nan

    return stg_df[out_cols]


def calculate_rolling_median(input_df,
                             input_cols: list,
                             number_of_periods=8):
    # calculate the median change rate of the trailing 12 months' data
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
            # print(f'_calculate_rolling_median Error: not all inputs are available = {col}')
            stg_df[out_var] = np.nan

    return stg_df[out_cols]


def calculate_rolling_stdev(input_df,
                            input_cols: list,
                            number_of_periods=8):
    # calculate the standard deviation change rate of the trailing 12 months' data
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
            # print(f'_calculate_rolling_stdev Error: not all inputs are available = {col}')
            stg_df[out_var] = np.nan

    return stg_df[out_cols]
