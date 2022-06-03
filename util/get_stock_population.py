from configs import database_configs_nas as dbcfg
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
                 WHERE active_ind = 'A' 
            """

        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.yahoo_ticker.to_list()

    def get_yahoo_ticker_from_screener(self, ticker_type='EQUITY'):
        sql = f"""
                SELECT distinct ticker
                FROM yahoo_universe
                WHERE quoteType='{ticker_type}'
            """
        df = pd.read_sql(con=self.cnn, sql=sql)

        return df.ticker.to_list()

    def all_stocks_wo_ETF_RIET(self):
        pop1 = self.get_stock_list()
        pop2 = self.get_stock_list_from_arron()

        return list(set(pop1+pop2))

    def yahoo_screener(self):
        pop5 = self.get_yahoo_ticker_from_screener(ticker_type='EQUITY')
        pop6 = self.get_yahoo_ticker_from_screener(ticker_type='ETF')

        return list(set(pop5+pop6))

    def all_stock(self):
        pop1 = self.get_stock_list()
        pop2 = self.get_stock_list_from_arron()
        pop3 = self.get_ETF_list()
        pop4 = self.get_REIT_list()
        pop5 = self.get_yahoo_ticker_from_screener(ticker_type='EQUITY')
        pop6 = self.get_yahoo_ticker_from_screener(ticker_type='ETF')

        return list(set(pop1 + pop2 + pop3 + pop4 + pop5 + pop6))


class SetPopulation:
    def __init__(self, user_pop):
        self.user_pop = user_pop
        self.saved_stock_pop = StockPopulation()

    def setPop(self):
        pop = {'ETF': self.saved_stock_pop.get_ETF_list(),
               'STOCK': self.saved_stock_pop.get_stock_list(),
               'AARON': self.saved_stock_pop.get_stock_list_from_arron(),
               'REIT': self.saved_stock_pop.get_REIT_list(),
               'ALL': self.saved_stock_pop.all_stock(),
               'YAHOO_SCREENER': self.saved_stock_pop.yahoo_screener(),
               'STOCK+AARON': self.saved_stock_pop.all_stocks_wo_ETF_RIET(),
               'YAHOO_STOCK_ALL': list(set(self.saved_stock_pop.get_yahoo_ticker_from_screener('EQUITY')
                                           + self.saved_stock_pop.get_stock_list_from_arron())
                                       ),
               'YAHOO_ETF_ALL': self.saved_stock_pop.get_yahoo_ticker_from_screener('ETF')
               }
        if pop.get(self.user_pop) is not None:
            return pop[self.user_pop]
        else:
            return []


if __name__ == '__main__':
    obj = SetPopulation('YHAOO_STOCK_ALL').setPop()
    print(obj)
