from modules.extract_yahoo_financials import YahooFinancial
from util.create_output_sqls import write_insert_db
import datetime
from util.gcp_functions import upload_to_bucket
from util.helper_functions import create_log
from configs import job_configs as jcfg
import os

loggerFileName = f"yahoo_financial_statements_{datetime.date.today().strftime('%Y%m%d')}.log"

create_log(loggerName='financial_statement_job',
           loggerFileName=loggerFileName)

runtime = datetime.datetime.today().date()

print("Extracting Financial Statements")
spider3 = YahooFinancial(updated_dt=runtime, batch=True, loggerFileName=loggerFileName, use_tqdm=False)
spider3.run()

outputs = ['yahoo_annual_fundamental', 'yahoo_quarterly_fundamental', 'yahoo_trailing_fundamental']
for sql_out in outputs:
    write_insert_db(sql_out, runtime).run_insert()


print("Start Uploading Files to GCP")
items = [f'insert_{file}_{runtime}.sql' for file in outputs]
for each_item in items:
    if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
        print("Successful: GCP upload successful for file = {}".format(each_item))
    else:
        print("Failed: GCP upload failed for file = {}".format(each_item))
