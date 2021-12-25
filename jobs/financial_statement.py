from modules.extract_yahoo_financial import YahooFinancial
from util.create_output_sqls import write_insert_db
import datetime
from util.gcp_functions import upload_to_bucket
from util.helper_functions import create_log
from configs import job_configs as jcfg
import os

loggerFileName = f"yahoo_stats_{datetime.date.today().strftime('%Y%m%d')}.log"

create_log(loggerName='daily_job',
           loggerFileName=loggerFileName)

runtime = datetime.datetime.today().date()
# runtime = '2021-11-12'

print("Extracting Financial Statements")
spider3 = YahooFinancial(updated_dt=runtime, batch=True, loggerFileName=loggerFileName)
spider3.run()

insert = write_insert_db('yahoo_annual_fundamental', runtime)
insert.run_INSERT()

insert = write_insert_db('yahoo_quarterly_fundamental', runtime)
insert.run_INSERT()

insert = write_insert_db('yahoo_trailing_fundamental', runtime)
insert.run_INSERT()


print("Start Uploading Files to GCP")
items = os.listdir(os.path.join(jcfg.JOB_ROOT, "sql_outputs"))
for each_item in items:
    if each_item.endswith("{}.sql".format(runtime)):
        if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
            print("GCP upload successful for file = {}".format(each_item))
        else:
            print("!!!!!!!!!GCP upload failed for file = {}".format(each_item))
