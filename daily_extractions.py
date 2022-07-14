from modules.extract_yahoo_stats import YahooStats
from modules.extract_etf_stats import YahooETF
from util.transfer_data import UploadData2GCP
from util.helper_functions import create_log
import datetime
import time
import sys


def DailyExtractions(runtime):
    loggerFileName = f"daily_job_{datetime.date.today().strftime('%Y%m%d')}_{int(time.time())}.log"

    create_log(loggerName='daily_job', loggerFileName=loggerFileName)

    sys.stderr.write(f"{'*' * 80}\n")
    sys.stderr.write(f'Daily job started for {runtime}\n')
    sys.stderr.write(f"{'-' * 80}\n")
    sys.stderr.write("Extracting Yahoo Statistics\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_fundamental`\n')

    # Call the Yahoo Statistics module
    stock_ext = YahooStats(runtime,
                           targeted_pop='YAHOO_STOCK_ALL',
                           batch=True,
                           loggerFileName=loggerFileName,
                           use_tqdm=False)
    stock_ext.run()

    sys.stderr.write(f"{'-' * 80}\n")
    sys.stderr.write("Extracting Yahoo ETF Stats\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_etf_prices`\n'
                     '    --`yahoo_etf_3y5y10y_risk`\n'
                     '    --`yahoo_etf_annual_returns`\n'
                     '    --`yahoo_etf_holdings`\n'
                     '    --`yahoo_etf_trailing_returns`\n')

    # # Call the Yahoo ETF
    etf = YahooETF(runtime,
                   targeted_pop='YAHOO_ETF_ALL',
                   batch=True,
                   loggerFileName=loggerFileName,
                   use_tqdm=False)
    etf.run()

    # upload the data
    sys.stderr.write(f"{'*' * 80}\n")
    sys.stderr.write("Upload data to the cloud \n")
    tables = ['yahoo_etf_3y5y10y_risk',
              'yahoo_etf_annual_returns',
              'yahoo_etf_holdings',
              'yahoo_etf_prices',
              'yahoo_etf_trailing_returns',
              'yahoo_fundamental']
    UploadData2GCP(tables).run()


if __name__ == '__main__':
    runtime = datetime.datetime.today().date() - datetime.timedelta(days=0)
    DailyExtractions(runtime)
