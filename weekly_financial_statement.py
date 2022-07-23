from modules.extract_yahoo_financials import YahooFinancial
import datetime
from util.transfer_data import UploadData2GCP
from util.helper_functions import create_log
from util.send_email import SendEmail
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

    SendEmail(content=f"""{'-'*80}\n"""
                      f'Weekly Yahoo Equity Financial Statement job finished for {run_time}\n'
                      f"""{'-'*80}\n"""
                      f"""{spider.get_failed_extracts}/{spider.no_of_stock} Failed Extractions. \n"""
                      f"""The job made {spider.no_of_web_calls} calls through the internet. \n"""
                      f"""The job made {spider.no_of_db_entries} entries to the database"""
                      f"""Target Table: yahoo_annual_fundamental\n"""
                      f"""              yahoo_quarterly_fundamental\n"""
                      f"""              yahoo_trailing_fundamental\n"""
                      f"""Target Population: YAHOO_STOCK_ALL\n"""
                      f"""Log: {loggerFileName}\n"""
                      f"""{'-'*80}\n""",
              subject=f'[Do not reply]Weekly Yahoo Equity Financial Statement Job has Finished - {run_time}').send()

    sys.stderr.write(f"Extracting Job is Completed, log is produced as {loggerFileName}\n")
    sys.stderr.write(f"{'*' * 80}\n")

    UploadData2GCP(['yahoo_annual_fundamental', 'yahoo_quarterly_fundamental', 'yahoo_trailing_fundamental'])


if __name__ == '__main__':
    run_time = datetime.datetime.today().date()
    main(run_time)
