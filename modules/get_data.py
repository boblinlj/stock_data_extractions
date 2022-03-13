from util.database_management import DatabaseManagement, DatabaseManagementError
from datetime import timedelta


class DataExtractError(Exception):
    pass


def get_price_data(ticker,
                   start_dt=None,
                   end_dt=None):
    if start_dt is None or end_dt is None:
        raise DataExtractError(f"Unable to extract the {ticker} price data due to missing start_dt or end_dt")

    my_sql = f"""
            SELECT timestamp as date, ticker, adjclose as price
            FROM price
            WHERE ticker = '{ticker}' AND timestamp BETWEEN '{start_dt}' AND '{end_dt}'
            ORDER BY timestamp
    """

    try:
        return DatabaseManagement(sql=my_sql).run_sql()
    except Exception:
        raise DataExtractError(f"Failed to run query {my_sql}")


def get_fundamental_data(ticker,
                         fundamental_lst: list,
                         start_dt=None,
                         end_dt=None):
    if len(fundamental_lst) < 1:
        raise DataExtractError(f'Unable to extract the data since fundamental_lst is empty.')

    my_sql = f"""
            SELECT updated_dt as date, ticker, {','.join(fundamental_lst)}
            FROM yahoo_fundamental
            WHERE ticker = '{ticker}' AND updated_dt BETWEEN '{start_dt}' AND '{end_dt}'
            ORDER BY updated_dt
    """
    try:
        return DatabaseManagement(sql=my_sql).run_sql()
    except Exception:
        raise DataExtractError(f"Failed to run query {my_sql}")


def get_columns_from_table(table):
    sql = f"""SHOW COLUMNS FROM {table}"""

    try:
        return DatabaseManagement(sql=sql).run_sql()
    except Exception:
        raise DataExtractError(f"Failed to run query {sql}")


def get_quarterly_statement(ticker,
                            element_lst: list,
                            from_dt=None,
                            to_dt=None,
                            latest_n=None):

    if len(element_lst) < 1:
        raise DataExtractError(f'Unable to extract the data since element_lst is empty')

    if latest_n is None:
        sql = f"""
                SELECT ticker, asOfDate, {','.join(element_lst)}
                FROM yahoo_quarterly_fundamental
                WHERE ticker = '{ticker}' AND asOfDate between '{from_dt}' and '{to_dt}'
            """
    else:
        sql = f"""
                SELECT ticker, asOfDate, {','.join(element_lst)}
                FROM yahoo_quarterly_fundamental
                WHERE ticker = '{ticker}'
                ORDER BY asOfDate DESC
                LIMIT {latest_n}
            """
    try:
        return DatabaseManagement(sql=sql).run_sql()
    except Exception:
        raise DataExtractError(f"Failed to run query {sql}")


if __name__ == '__main__':
    print(get_quarterly_statement('AAPL', [], '2021-01-01', '2022-12-31', 4))
