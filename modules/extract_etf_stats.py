from configs import yahoo_configs as ycfg
from configs import job_configs as jcfg
from datetime import date
import json
import pandas as pd
import numpy as np
import time
from util.helper_functions import create_log
from util.helper_functions import unix_to_regular_time
from util.helper_functions import dedup_list
from util.helper_functions import returnNotMatches
from util.request_website import YahooAPIParser


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

            return final_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def trailingReturns(self):
        try:
            etf_ttm_r = self.js['quoteSummary']['result'][0]['fundPerformance']['trailingReturns']
            etf_ttm_r_df = pd.DataFrame.from_records(etf_ttm_r).loc['raw']
            etf_ttm_r_df['asOfDate'] = unix_to_regular_time(etf_ttm_r_df['asOfDate'])
            return etf_ttm_r_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def riskOverviewStatistics(self):
        try:
            etf_risk = self.js['quoteSummary']['result'][0]['fundPerformance']['riskOverviewStatistics']
            etf_risk_df = pd.DataFrame.from_records(etf_risk.get('riskStatistics'))
            for col in etf_risk_df.columns:
                if col != 'year':
                    etf_risk_df[col] = etf_risk_df[col].apply(lambda x: x.get('raw'))
            return etf_risk_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def topHoldings(self):
        try:
            etf_holdings = self.js['quoteSummary']['result'][0]['topHoldings']

            sector = {}
            for i in etf_holdings.get('sectorWeightings'):
                for key, item in i.items():
                    sector[key] = item.get('raw')

            bond_rating = {}
            for i in etf_holdings.get('bondRatings'):
                for key, item in i.items():
                    bond_rating[key] = item.get('raw')

            holding_stats = {
                'cashPosition': etf_holdings['cashPosition'].get('raw'),
                'stockPosition': etf_holdings['stockPosition'].get('raw'),
                'bondPosition': etf_holdings['bondPosition'].get('raw'),
                'otherPosition': etf_holdings['otherPosition'].get('raw'),
                'preferredPosition': etf_holdings['preferredPosition'].get('raw'),
                'convertiblePosition': etf_holdings['convertiblePosition'].get('raw'),
                'priceToEarnings': etf_holdings['equityHoldings']['priceToEarnings'].get('raw'),
                'priceToBook': etf_holdings['equityHoldings']['priceToBook'].get('raw'),
                'priceToSales': etf_holdings['equityHoldings']['priceToSales'].get('raw'),
                'priceToCashflow': etf_holdings['equityHoldings']['priceToCashflow'].get('raw'),
                'threeYearEarningsGrowth': etf_holdings['equityHoldings']['threeYearEarningsGrowth'].get('raw'),
                'creditQuality': etf_holdings['bondHoldings']['creditQuality'].get('raw'),
                'duration': etf_holdings['bondHoldings']['duration'].get('raw'),
                'maturity': etf_holdings['bondHoldings']['maturity'].get('raw'),
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
            etf_price_df = pd.DataFrame.from_records(etf_price).loc['raw']
            return etf_price_df
        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")


class YahooETF:
    yahoo_module = ['topHoldings', 'fundPerformance', 'price']
    BASE_URL = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{stock}?modules=' + '%2C'.join(yahoo_module)
    workers = jcfg.WORKER
    failed_extract = []

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None, use_tqdm=True):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooETFStats', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm

    def _get_etf_statistics(self, stock):
        data = YahooAPIParser(url=self.BASE_URL.format(stock=stock)).parse()
        # read data from Yahoo API
        readData = ReadYahooETFStatData(data)
        annualTotalReturns = readData.annualTotalReturns()
        annualTotalReturns['ticker'] = stock
        annualTotalReturns['updated_dt'] = self.updated_dt

        trailingReturns = readData.trailingReturns()
        trailingReturns['ticker'] = stock
        trailingReturns['updated_dt'] = self.updated_dt

        riskOverviewStatistics = readData.riskOverviewStatistics()

        topHoldings = readData.topHoldings()
        topHoldings['ticker'] = stock
        topHoldings['updated_dt'] = self.updated_dt

        price = readData.price()
        price['ticker'] = stock
        price['updated_dt'] = self.updated_dt

        print(riskOverviewStatistics)


if __name__ == '__main__':
    obj = YahooETF('2022-04-02', targeted_pop='YAHOO_ETF_ALL')
    obj._get_etf_statistics('BND')



