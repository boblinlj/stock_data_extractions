from modules.extract_yahoo_financials import YahooFinancial
import datetime
from util.transfer_data import UploadData2GCP
from util.helper_functions import create_log
import sys


def main(run_time):
    loggerFileName = f"yahoo_financial_statements_{datetime.date.today().strftime('%Y%m%d')}.log"

    create_log(loggerName='yahoo_financials_', loggerFileName=loggerFileName)

    sys.stderr.write(f"{'*' * 80}\n")
    sys.stderr.write(f'Weekly job started for {run_time}\n')
    sys.stderr.write(f"{'-' * 80}\n")
    sys.stderr.write("Extracting Yahoo Financial Statement Data\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_annual_fundamental`\n'
                     '    --`yahoo_quarterly_fundamental`\n'
                     '    --`yahoo_trailing_fundamental`\n')

    # Call the job module
    spider = YahooFinancial(run_time,
                            targeted_pop='YAHOO_STOCK_ALL',
                            batch=True,
                            loggerFileName=loggerFileName,
                            use_tqdm=True)
    spider.run()

    sys.stderr.write(f"Extracting Job is Completed, log is produced as {loggerFileName}\n")
    sys.stderr.write(f"{'*' * 80}\n")

    UploadData2GCP(['yahoo_annual_fundamental', 'yahoo_quarterly_fundamental', 'yahoo_trailing_fundamental'])


if __name__ == '__main__':
    run_time = datetime.datetime.today().date()
    main(run_time)
