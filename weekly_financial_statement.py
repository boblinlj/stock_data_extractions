from modules.extract_yahoo_financials import YahooFinancial
import datetime
from util.gcp_functions import upload_sql_to_GCP_cloud_storage
from util.helper_functions import create_log
import sys


def DailyExtractions(runtime):
    loggerFileName = f"yahoo_financial_statements_{datetime.date.today().strftime('%Y%m%d')}.log"

    create_log(loggerName='yahoo_financials_', loggerFileName=loggerFileName)

    sys.stderr.write(f"{'*'*80}\n")
    sys.stderr.write(f'Weekly job started for {runtime}\n')
    sys.stderr.write(f"{'-'*80}\n")
    sys.stderr.write("Extracting Yahoo Financial Statement Data\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_annual_fundamental`\n'
                     '    --`yahoo_quarterly_fundamental`\n'
                     '    --`yahoo_trailing_fundamental`\n')
    # Call the job module
    spider2 = YahooFinancial(runtime,
                             targeted_pop='YAHOO_STOCK_ALL',
                             batch=True,
                             loggerFileName=loggerFileName,
                             use_tqdm=False)
    spider2.run()

    sys.stderr.write(f"Extracting Job is Completed, log is produced as {loggerFileName}\n")
    sys.stderr.write(f"{'*'*80}\n")

    upload_sql_to_GCP_cloud_storage(['yahoo_annual_fundamental',
                                     'yahoo_quarterly_fundamental',
                                     'yahoo_trailing_fundamental'],
                                    runtime)


runtime = datetime.datetime.today().date() - datetime.timedelta(days=1)
DailyExtractions(runtime)

