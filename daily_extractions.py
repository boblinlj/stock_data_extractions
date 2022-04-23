import sys
from modules.extract_yahoo_stats import YahooStats
from modules.extract_etf_stats import YahooETF
import datetime
from util.gcp_functions import upload_sql_to_GCP_cloud_storage
from util.helper_functions import create_log


def DailyExtractions(runtime):
    loggerFileName = f"daily_job_{datetime.date.today().strftime('%Y%m%d')}.log"

    create_log(loggerName='daily_job', loggerFileName=loggerFileName)

    sys.stderr.write(f"{'*'*80}\n")
    sys.stderr.write(f'Daily job started for {runtime}\n')
    sys.stderr.write(f"{'-'*80}\n")
    sys.stderr.write("Extracting Yahoo Statistics\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_fundamental`\n')
    # Call the Yahoo Statistics module
    # spider2 = YahooStats(runtime,
    #                      targeted_pop='YAHOO_STOCK_ALL',
    #                      batch=True,
    #                      loggerFileName=loggerFileName,
    #                      use_tqdm=False)
    # spider2.run()

    sys.stderr.write(f"{'-'*80}\n")
    sys.stderr.write("Extracting Yahoo ETF Stats\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_etf_prices`\n'
                     '    --`yahoo_etf_3y5y10y_risk`\n'
                     '    --`yahoo_etf_annual_returns`\n'
                     '    --`yahoo_etf_holdings`\n'
                     '    --`yahoo_etf_trailing_returns`\n')
    # Call the Yahoo ETF
    # etf = YahooETF(runtime,
    #                targeted_pop='YAHOO_ETF_ALL',
    #                batch=True,
    #                loggerFileName=loggerFileName,
    #                use_tqdm=False)
    # etf.run()

    # upload the data
    sys.stderr.write(f"Extracting Job is Completed, log is produced as {loggerFileName}\n")
    sys.stderr.write(f"{'*'*80}\n")
    upload_sql_to_GCP_cloud_storage(['yahoo_fundamental',
                                     'yahoo_etf_prices',
                                     'yahoo_etf_3y5y10y_risk',
                                     'yahoo_etf_annual_returns',
                                     'yahoo_etf_holdings',
                                     'yahoo_etf_trailing_returns'], runtime)


# runtime = datetime.datetime.today().date()
runtime = '2022-04-18'
DailyExtractions(runtime)
