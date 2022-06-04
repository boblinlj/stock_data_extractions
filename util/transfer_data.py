from sqlalchemy import create_engine
import pandas as pd
import configs.database_configs_nas as dcf_nas
import configs.database_configs_prod as dcf_prod


class adhoc_uplaod:
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

    def __init__(self, table):
        self.table = table

    def find_the_latest_entry(self):
        sql = f"""select max(updated_dt) as updated_dt from {self.table}"""
        return pd.read_sql(sql=sql, con=self.cnn_to).iloc[0, 0]

    def upload_the_data(self, df):
        df.to_sql(name=self.table, con=self.cnn_to,
                  if_exists='append',
                  index=False,
                  chunksize=2000)

    def days_to_extract(self, latest_dt):
        sql_days = f"""select distinct updated_dt from {self.table} where updated_dt > '{latest_dt}'"""
        return pd.read_sql(sql=sql_days, con=self.cnn_from).values.tolist()

    def extract_data(self, updated_dt):
        sql = f"""select * from {self.table} where updated_dt = '{updated_dt}' """
        return pd.read_sql(sql=sql, con=self.cnn_from)

    def run(self):
        days = self.days_to_extract(self.find_the_latest_entry())
        if len(days) <= 0:
            print(f"{self.table} does not have new data")
        for i in days[:]:
            df = self.extract_data(i[0])
            print(f"{self.table} is updated for {i[0]} with {df.shape[0]} records")
            self.upload_the_data(df=df)


if __name__ == '__main__':

    for table in ['yahoo_annual_fundamental',
                  'yahoo_etf_3y5y10y_risk',
                  'yahoo_etf_annual_returns',
                  'yahoo_etf_holdings',
                  'yahoo_etf_prices',
                  'yahoo_etf_trailing_returns',
                  'yahoo_quarterly_fundamental',
                  'yahoo_trailing_fundamental',
                  'yahoo_fundamental'
                  ]:
        adhoc_uplaod(table).run()
