import time
from configs import job_configs as jcfg
import pandas as pd
from util.helper_functions import create_log,unix_to_regular_time, dedup_list
from util.get_stock_population import SetPopulation
from util.parallel_process import parallel_process
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement


class YahooETFExtractionError(Exception):
    pass


class ReadYahooETFStatData:
    parse_items = [
        'trailingReturns',
        'trailingReturnsCat',
        'annualTotalReturns',
        'riskOverviewStatistics',
        'riskOverviewStatisticsCat'
    ]

    def __init__(self, js):
        self.js = js

    def preliminary_check(self):
        try:
            var1 = self.js['quoteSummary']['result'][0]['fundPerformance']
            return True
        except KeyError:
            return False

    def annualTotalReturns(self):
        try:
            etf_r = self.js['quoteSummary']['result'][0]['fundPerformance']['annualTotalReturns']['returns']
            bk_r = self.js['quoteSummary']['result'][0]['fundPerformance']['annualTotalReturns']['returnsCat']

            etf_r_df = pd.DataFrame.from_records(etf_r, index='year')
            etf_r_df['etfAnnualReturn'] = etf_r_df['annualValue'].apply(lambda x: x.get('raw'))
            etf_r_df.drop(columns=['annualValue'], inplace=True)

            bk_r_df = pd.DataFrame.from_records(bk_r, index='year')
            bk_r_df['benchmarkAnnualReturn'] = bk_r_df['annualValue'].apply(lambda x: x.get('raw'))
            bk_r_df.drop(columns=['annualValue'], inplace=True)

            final_df = etf_r_df.join(bk_r_df, how='left')
            final_df.reset_index(inplace=True)

            return final_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def trailingReturns(self):
        try:
            etf_ttm_r = self.js['quoteSummary']['result'][0]['fundPerformance']['trailingReturns']
            etf_ttm_r_df = pd.DataFrame.from_records(etf_ttm_r).loc['raw']
            etf_ttm_r_df['asOfDate'] = unix_to_regular_time(etf_ttm_r_df['asOfDate'])
            return etf_ttm_r_df.to_frame().transpose()

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def riskOverviewStatistics(self):
        try:
            etf_risk = self.js['quoteSummary']['result'][0]['fundPerformance']['riskOverviewStatistics']
            etf_risk_df = pd.DataFrame.from_records(etf_risk.get('riskStatistics'))
            for col in etf_risk_df.columns:
                if col != 'year':
                    etf_risk_df[col] = etf_risk_df[col].apply(lambda x: x.get('raw'))

            df_5y = etf_risk_df.loc[etf_risk_df['year'] == '5y'][:]
            df_5y.columns = [x + '_5y' for x in df_5y.columns]
            df_5y.reset_index(inplace=True)
            df_5y.drop(columns=['year_5y', 'index'], inplace=True)

            df_3y = etf_risk_df.loc[etf_risk_df['year'] == '3y'][:]
            df_3y.columns = [x + '_3y' for x in df_3y.columns]
            df_3y.reset_index(inplace=True)
            df_3y.drop(columns=['year_3y', 'index'], inplace=True)

            df_10y = etf_risk_df.loc[etf_risk_df['year'] == '10y'][:]
            df_10y.columns = [x + '_10y' for x in df_10y.columns]
            df_10y.reset_index(inplace=True)
            df_10y.drop(columns=['year_10y', 'index'], inplace=True)

            output_df = pd.concat([df_3y, df_5y, df_10y], axis=1, ignore_index=False)

            return output_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def topHoldings(self):
        try:
            etf_holdings = self.js['quoteSummary']['result'][0]['topHoldings']

            sector = {}
            for i in etf_holdings.get('sectorWeightings'):
                for key, item in i.items():
                    sector['sector_weight_' + key] = item.get('raw')

            bond_rating = {}
            for i in etf_holdings.get('bondRatings'):
                for key, item in i.items():
                    bond_rating['bond_rating_' + key] = item.get('raw')

            holding_stats = {
                'cashPosition': etf_holdings['cashPosition'].get('raw'),
                'stockPosition': etf_holdings['stockPosition'].get('raw'),
                'bondPosition': etf_holdings['bondPosition'].get('raw'),
                'otherPosition': etf_holdings['otherPosition'].get('raw'),
                'preferredPosition': etf_holdings['preferredPosition'].get('raw'),
                'convertiblePosition': etf_holdings['convertiblePosition'].get('raw'),
                'Equity_priceToEarnings': etf_holdings['equityHoldings']['priceToEarnings'].get('raw'),
                'Equity_priceToBook': etf_holdings['equityHoldings']['priceToBook'].get('raw'),
                'Equity_priceToSales': etf_holdings['equityHoldings']['priceToSales'].get('raw'),
                'Equity_priceToCashflow': etf_holdings['equityHoldings']['priceToCashflow'].get('raw'),
                'Equity_threeYearEarningsGrowth': etf_holdings['equityHoldings']['threeYearEarningsGrowth'].get('raw'),
                'Bond_creditQuality': etf_holdings['bondHoldings']['creditQuality'].get('raw'),
                'Bond_duration': etf_holdings['bondHoldings']['duration'].get('raw'),
                'Bond_maturity': etf_holdings['bondHoldings']['maturity'].get('raw'),
            }

            sector_df = pd.DataFrame(data=sector, index=[0])
            bond_rating_df = pd.DataFrame(data=bond_rating, index=[0])
            holding_stats_df = pd.DataFrame(data=holding_stats, index=[0])

            final_output = pd.concat([sector_df, bond_rating_df, holding_stats_df], axis=1)

            return final_output
        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def price(self):
        try:
            etf_price = self.js['quoteSummary']['result'][0]['price']
            etf_price_df = pd.DataFrame.from_records(etf_price).loc['raw'][:]
            etf_price_df = etf_price_df.to_frame().transpose()
            drop_columns = ['index', 'currencySymbol', 'marketState', 'maxAge', 'postMarketSource', 'preMarketSource',
                            'quoteSourceName', 'regularMarketSource', 'symbol', 'postMarketTime', 'preMarketTime',
                            'regularMarketTime', 'toCurrency', 'underlyingSymbol', 'priceHint', 'circulatingSupply',
                            'fromCurrency', 'lastMarket', 'exchangeDataDelayedBy', 'strikePrice', 'volume24Hr',
                            'volumeAllCurrencies']
            etf_price_df.drop(columns=[x for x in drop_columns if x in etf_price_df.columns], inplace=True)
            return etf_price_df
        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")


class YahooETF:
    yahoo_module = ['topHoldings', 'fundPerformance', 'price']
    BASE_URL = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{stock}?modules=' + '%2C'.join(yahoo_module)
    workers = jcfg.WORKER

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None, use_tqdm=True):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooETFStats', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        # list to store failed extractions
        self.failed = []

    @staticmethod
    def _check_existing(stock, updated_dt, table, df_to_check):
        sql = f"""
                SELECT *
                FROM {table}
                WHERE ticker = '{stock}' and updated_dt = '{updated_dt}'
            """
        if df_to_check.empty:
            return df_to_check

        existing_df = DatabaseManagement(sql=sql).read_to_df()
        existing_df.drop(columns=['data_id', 'updated_dt', 'ticker'], inplace=True)

        if existing_df.empty:
            return df_to_check

        df_diff = pd.concat([existing_df, df_to_check]).drop_duplicates(subset=None, keep=False, inplace=True)

        if df_diff is None:
            return pd.DataFrame
        else:
            return df_diff

    def _get_etf_statistics(self, stock):
        data = YahooAPIParser(url=self.BASE_URL.format(stock=stock)).parse()
        # read data from Yahoo API
        readData = ReadYahooETFStatData(data)

        try:
            trailingReturns = readData.trailingReturns()
            trailingReturns = self._check_existing(stock, self.updated_dt, 'yahoo_etf_trailing_returns', trailingReturns)
            if not trailingReturns.empty:
                trailingReturns['ticker'] = stock
                trailingReturns['updated_dt'] = self.updated_dt

                DatabaseManagement(data_df=trailingReturns,
                                   table='yahoo_etf_trailing_returns',
                                   insert_index=False).insert_db()
        except YahooETFExtractionError:
            self.logger.debug(f"{stock}: trailingReturns extraction failed")
            self.failed.append(stock)

        try:
            riskOverviewStatistics = readData.riskOverviewStatistics()
            riskOverviewStatistics = self._check_existing(stock, self.updated_dt, 'yahoo_etf_3y5y10y_risk', riskOverviewStatistics)
            if not riskOverviewStatistics.empty:
                riskOverviewStatistics['ticker'] = stock
                riskOverviewStatistics['updated_dt'] = self.updated_dt
                DatabaseManagement(data_df=riskOverviewStatistics,
                                   table='yahoo_etf_3y5y10y_risk',
                                   insert_index=False).insert_db()
        except YahooETFExtractionError:
            self.logger.debug(f"{stock}: riskOverviewStatistics extraction failed")
            self.failed.append(stock)

        try:
            topHoldings = readData.topHoldings()
            topHoldings = self._check_existing(stock, self.updated_dt, 'yahoo_etf_holdings', topHoldings)
            if not topHoldings.empty:
                topHoldings['ticker'] = stock
                topHoldings['updated_dt'] = self.updated_dt
                DatabaseManagement(data_df=topHoldings,
                                   table='yahoo_etf_holdings',
                                   insert_index=False).insert_db()
        except YahooETFExtractionError:
            self.logger.debug(f"{stock}: topHoldings extraction failed")
            self.failed.append(stock)

        try:
            price = readData.price()
            price = self._check_existing(stock, self.updated_dt, 'yahoo_etf_prices', price)
            if not price.empty:
                price['ticker'] = stock
                price['updated_dt'] = self.updated_dt
                DatabaseManagement(data_df=price,
                                   table='yahoo_etf_prices',
                                   insert_index=False).insert_db()
        except YahooETFExtractionError:
            self.logger.debug(f"{stock}: price extraction failed")
            self.failed.append(stock)

        try:
            price = readData.annualTotalReturns()
            price = self._check_existing(stock, self.updated_dt, 'yahoo_etf_annual_returns', price)
            if not price.empty:
                price['ticker'] = stock
                price['updated_dt'] = self.updated_dt
                DatabaseManagement(data_df=price,
                                   table='yahoo_etf_annual_returns',
                                   insert_index=False).insert_db()
        except YahooETFExtractionError:
            self.logger.debug(f"{stock}: yahoo_etf_annual_returns extraction failed")
            self.failed.append(stock)

        self.logger.info(f"{stock}: processed successfully")

    def run(self):
        start = time.time()

        self.logger.info(f"{'*'*40}1st Run{'*'*40}")
        stocks = SetPopulation(self.targeted_pop).setPop()
        if self.batch:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=1, use_tqdm=self.use_tqdm)

        self.logger.info(f"{'*'*40}2rd Run{'*'*40}")
        stocks = dedup_list(self.failed)
        self.failed = []
        if self.batch:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=1, use_tqdm=self.use_tqdm)

        self.logger.info(f"{'*'*40}3rd Run{'*'*40}")
        stocks = dedup_list(self.failed)
        self.failed = []
        if self.batch:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=self.workers, use_tqdm=self.use_tqdm)
        else:
            parallel_process(stocks, self._get_etf_statistics, n_jobs=1, use_tqdm=self.use_tqdm)

        end = time.time()
        self.logger.info("took {} minutes".format(round((end - start) / 60)))


if __name__ == '__main__':
    obj = YahooETF('2022-04-12',
                   targeted_pop='YAHOO_ETF_ALL',
                   batch=True,
                   loggerFileName=None,
                   use_tqdm=True)
    obj._get_etf_statistics('BND')
