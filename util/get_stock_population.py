from configs import database_configs as dbcfg
from sqlalchemy import create_engine
import pandas as pd


class StockPopulation:

    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    def get_stock_list(self):
        sql = """SELECT DISTINCT ticker 
                    FROM `finviz_tickers` 
                    where industry not like "REIT%" and industry <> "Exchange Traded Fund" 
                        and updated_dt = (SELECT MAX(updated_dt) FROM finviz_tickers) 
                    ORDER BY volume DESC, market_cap desc"""

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def get_ETF_list(self):
        sql = """SELECT DISTINCT ticker 
                 FROM `finviz_tickers` 
                 WHERE industry = "Exchange Traded Fund" 
                        and updated_dt = (SELECT MAX(updated_dt) FROM finviz_tickers) 
                 ORDER BY volume DESC, market_cap desc"""

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def get_REIT_list(self):
        sql = """SELECT DISTINCT ticker 
                 FROM `finviz_tickers` 
                 WHERE industry like "REIT%"
                        and updated_dt = (SELECT max(updated_dt) FROM finviz_tickers) 
                 ORDER BY volume DESC, market_cap desc"""

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def get_stock_list_from_arron(self):
        sql = """SELECT DISTINCT yahoo_ticker 
                 FROM stock_list_for_cooble_stone 
                 WHERE active_ind = 'A' """

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.yahoo_ticker.to_list()

    def all_stocks_wo_ETF_RIET(self):
        pop1 = self.get_stock_list()
        pop2 = self.get_stock_list_from_arron()

        return list(set(pop1+pop2))

    def all_stock(self):
        pop1 = self.get_stock_list()
        pop2 = self.get_stock_list_from_arron()
        pop3 = self.get_ETF_list()
        pop4 = self.get_REIT_list()

        return list(set(pop1 + pop2 + pop3 + pop4))
