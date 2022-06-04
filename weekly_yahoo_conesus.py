import datetime
from util.helper_functions import create_log
from util.transfer_data import UploadData2GCP
from modules.extract_yahoo_consensus import YahooAnalysis


def CensusExtractions(runtime):
    loggerFileName = f"Weekly_Yahoo_Analysis_Job_{datetime.date.today().strftime('%Y%m%d')}.log"

    create_log(loggerName='Weekly_Yahoo_Analysis_Job', loggerFileName=loggerFileName)

    print(runtime)
    print("*" * 30)
    print("Extracting Yahoo Analysis Data")
    consus = YahooAnalysis(updated_dt=runtime,
                           targeted_pop='YAHOO_STOCK_ALL',
                           batch_run=True,
                           loggerFileName=loggerFileName,
                           use_tqdm=True)
    consus.run_job()

    UploadData2GCP(['yahoo_consensus'])


runtime = datetime.datetime.today().date()
CensusExtractions(runtime)
