from util.request_website import YahooWebParser, WebParseError
from util.get_stock_population import SetPopulation
from util.parallel_process import parallel_process
from util.database_management import DatabaseManagement
from util.helper_functions import returnNotMatches, create_log
import pandas as pd


class ExtractProfileError(Exception):
    pass


class ExtractProfile:
    BASE_URL = 'https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}'

    def __init__(self, population, updated_dt, proxy=True, loggerFileName=None):
        self.existing = None
        self.population = population
        self.proxy = proxy
        self.updated_dt = updated_dt
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName=f'ExtractProfile', loggerFileName=self.loggerFileName)
        self.population = SetPopulation(self.population).setPop()
        self.output_df = pd.DataFrame()
        self.existing = DatabaseManagement(table='yahoo_universe_profile',
                                           key='ticker',
                                           where=f"updated_dt = '{self.updated_dt}'").get_record().ticker.to_list()

    def get_profile_each_stock(self, stock):
        url = self.BASE_URL.format(ticker=stock)
        js = None
        try:
            js = YahooWebParser(url=url, proxy=self.proxy).parse()
        except WebParseError as e:
            self.logger.debug(e)

        try:
            if js is not None:
                assetProfile = js['context']['dispatcher']['stores']['QuoteSummaryStore']['assetProfile']
                quoteType = js['context']['dispatcher']['stores']['QuoteSummaryStore']['quoteType']
                df = pd.DataFrame.from_records({'ticker': stock,
                                                'industry': assetProfile.get('industry'),
                                                'sector': assetProfile.get('sector'),
                                                'exchange': quoteType.get('exchange'),
                                                'exchangeTimezoneName': quoteType.get('exchangeTimezoneName'),
                                                'market': quoteType.get('market'),
                                                'exchangeTimezoneShortName': quoteType.get('exchangeTimezoneShortName'),
                                                'updated_dt': self.updated_dt
                                                }, index=[0])

                DatabaseManagement(data_df=df, table='yahoo_universe_profile', insert_index=False).insert_db()
        except:
            self.logger.info(f"Unable to extract {stock}")

    def extract_the_entire_population(self):
        print(len(self.population))
        print(len(self.existing))
        final_pop = returnNotMatches(self.population, self.existing)
        print(len(final_pop))

        if len(final_pop) <= 0:
            raise ExtractProfileError(f"Population is empty, check if it were valid.")
        else:
            parallel_process(final_pop, self.get_profile_each_stock, n_jobs=30, use_tqdm=True)


if __name__ == '__main__':
    obj = ExtractProfile('YAHOO_SCREENER', updated_dt='2022-03-21', proxy=True)
    obj.extract_the_entire_population()
