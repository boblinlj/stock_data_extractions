from sqlalchemy import create_engine
import pandas as pd
import configs.database_configs_nas as dcf_nas


class adhoc_uplaod:
    database_ip = dcf_nas.MYSQL_HOST
    database_user = dcf_nas.MYSQL_USER
    database_pw = dcf_nas.MYSQL_PASSWORD
    database_port = dcf_nas.MYSQL_PORT
    database_nm = dcf_nas.MYSQL_DATABASE

    cnn_from = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                             pool_size=20,
                             max_overflow=0)

    cnn_to = create_engine(f'mysql+mysqlconnector://boblin:Zuodan199064@34.70.76.153:3306/financial_PROD',
                           pool_size=20,
                           max_overflow=0)

    def __init__(self, table):
        self.table = table

    def find_the_latest_entry(self):
        sql = f"""select max(updated_dt) as updated_dt from {self.table}"""
        return pd.read_sql(sql=sql, con=self.cnn_to).iloc[0, 0]

    def upload_the_data(self, df):
        df.to_sql(name=self.table,
                  con=self.cnn_to,
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
        for i in days[:]:
            df = self.extract_data(i[0])
            self.upload_the_data(df=df)


if __name__ == '__main__':

    for table in ['yahoo_annual_fundamental',
                  'yahoo_etf_3y5y10y_risk',
                  'yahoo_etf_annual_returns',
                  'yahoo_etf_holdings',
                  'yahoo_etf_prices',
                  'yahoo_etf_trailing_returns',
                  'yahoo_quarterly_fundamental',
                  'yahoo_trailing_fundamental'
                  ]:
        print(table)
        adhoc_uplaod(table).run()
