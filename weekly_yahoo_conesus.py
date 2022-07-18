import datetime
import sys
from util.helper_functions import create_log
from util.transfer_data import UploadData2GCP
from util.send_email import SendEmail
from modules.extract_yahoo_consensus import YahooAnalysis


def CensusExtractions(runtime):
    loggerFileName = f"Weekly_Yahoo_Analysis_Job_{datetime.date.today().strftime('%Y%m%d')}.log"

    create_log(loggerName='Weekly_Yahoo_Analysis_Job', loggerFileName=loggerFileName)

    sys.stderr.write(f"{'*' * 80}\n")
    sys.stderr.write(f'Weekly job started for {runtime}\n')
    sys.stderr.write(f"{'-' * 80}\n")
    sys.stderr.write("Extracting Yahoo Consensus\n")
    sys.stderr.write('This job will population tables: \n'
                     '    --`yahoo_consensus`\n')
    sys.stderr.write(f"{'*' * 80}\n")

    consus = YahooAnalysis(updated_dt=runtime,
                           targeted_pop='YAHOO_STOCK_ALL',
                           batch_run=True,
                           loggerFileName=loggerFileName,
                           use_tqdm=True)
    consus.run_job()

    SendEmail(content=f"""{'-'*80}\n"""
                      f'Weekly consensus job finished for {runtime}\n'
                      f"""{'-'*80}\n"""
                      f"""{consus.get_failed_extracts}/{consus.no_of_stock} Failed Extractions. \n"""
                      f"""The job made {consus.no_of_web_calls} calls through the internet. \n"""
                      f"""Target Table: yahoo_consensus\n"""
                      f"""Target Population: YAHOO_STOCK_ALL\n"""
                      f"""Log: {loggerFileName}\n"""
                      f"""{'-'*80}\n""",
              subject=f'Weekly consensus job has Finished - {runtime}').send()

    UploadData2GCP(['yahoo_consensus'], loggerFileName=loggerFileName)


runtime = datetime.datetime.today().date()
CensusExtractions(runtime)
