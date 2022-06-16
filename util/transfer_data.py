from sqlalchemy import create_engine
import pandas as pd
import configs.database_configs_nas as dcf_nas
import configs.database_configs_prod as dcf_prod
from util.helper_functions import create_log


class UploadData2GCP:
    database_ip = dcf_nas.MYSQL_HOST
    database_user = dcf_nas.MYSQL_USER
    database_pw = dcf_nas.MYSQL_PASSWORD
    database_port = dcf_nas.MYSQL_PORT
    database_nm = dcf_nas.MYSQL_DATABASE

    database_ip_prd = dcf_prod.MYSQL_HOST
    database_user_prd = dcf_prod.MYSQL_USER
    database_pw_prd = dcf_prod.MYSQL_PASSWORD
    database_port_prd = dcf_prod.MYSQL_PORT
    database_nm_prd = dcf_prod.MYSQL_DATABASE

    cnn_from = create_engine(
        f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
        pool_size=20,
        max_overflow=0)

    cnn_to = create_engine(
        f'mysql+mysqlconnector://{database_user_prd}:{database_pw_prd}@{database_ip_prd}:{database_port_prd}/{database_nm_prd}',
        pool_size=20,
        max_overflow=0)

    def __init__(self, table_to_upload: list, logger=None):
        self.table_to_upload = table_to_upload
        self.logger = logger

    def find_the_latest_entry(self, _each_table):
        sql = f"""select max(updated_dt) as updated_dt from {_each_table}"""
        return pd.read_sql(sql=sql, con=self.cnn_to).iloc[0, 0]

    def upload_the_data(self, df, _each_table):
        df.to_sql(name=_each_table, con=self.cnn_to,
                  if_exists='append',
                  index=False,
                  chunksize=2000)

    def days_to_extract(self, latest_dt, _each_table):
        sql_days = f"""select distinct updated_dt from {_each_table} where updated_dt > '{latest_dt}'"""
        return pd.read_sql(sql=sql_days, con=self.cnn_from).values.tolist()

    def extract_data(self, _each_table, updated_dt):
        sql = f"""select * from {_each_table} where updated_dt = '{updated_dt}' """
        return pd.read_sql(sql=sql, con=self.cnn_from)

    def run(self):
        for _each_table in self.table_to_upload:
            days = self.days_to_extract(self.find_the_latest_entry(_each_table), _each_table)

            # if there is no new days, ie no new data
            if len(days) <= 0:
                if self.logger:
                    self.logger.info(f"{_each_table} does not have new data")
                else:
                    print(f"{_each_table} does not have new data")

            # processing new data date one by one
            for i in days[:]:
                df = self.extract_data(_each_table, i[0])
                self.upload_the_data(df=df, _each_table=_each_table)
                if self.logger:
                    print(f"{_each_table} is updated for {i[0]} with {df.shape[0]} records")
                else:
                    self.logger.info(f"{_each_table} is updated for {i[0]} with {df.shape[0]} records")


if __name__ == '__main__':
    tables = ['yahoo_annual_fundamental',
              'yahoo_etf_3y5y10y_risk',
              'yahoo_etf_annual_returns',
              'yahoo_etf_holdings',
              'yahoo_etf_prices',
              'yahoo_etf_trailing_returns',
              'yahoo_quarterly_fundamental',
              'yahoo_trailing_fundamental',
              'yahoo_fundamental'
              ]
    UploadData2GCP(tables, None).run()
