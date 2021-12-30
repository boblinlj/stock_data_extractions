from modules.extract_finviz_data import Finviz
from modules.extract_yahoo_stats import YahooStats
from util.create_output_sqls import write_insert_db
import datetime
from util.gcp_functions import upload_to_bucket
from configs import job_configs as jcfg
import os
from util.helper_functions import create_log

loggerFileName = f"daily_job_{datetime.date.today().strftime('%Y%m%d')}.log"

create_log(loggerName='daily_job', loggerFileName=loggerFileName)

# runtime = datetime.datetime.today().date() - datetime.timedelta(days = 3)
runtime = datetime.datetime.today().date()
print(runtime)
print("*" * 30)
print("Extracting Finviz")
finviz = Finviz(runtime, loggerFileName=loggerFileName)
finviz.run()
print("*" * 30)
print("Extracting Yahoo Statistics")
spider2 = YahooStats(runtime, batch=True, loggerFileName=loggerFileName)
spider2.run()
print("*" * 30)

outputs = ['finviz_screener', 'finviz_tickers', 'yahoo_fundamental', 'yahoo_price', 'yahoo_consensus_price']
# generate sql script for upload
print("Start to generating output files")
print("*" * 30)
for sql_out in outputs:
    write_insert_db(sql_out, runtime).run_insert()


print("Start Uploading Files to GCP")
# items = os.listdir(os.path.join(jcfg.JOB_ROOT, "sql_outputs"))
items = [f'insert_{file}_{runtime}' for file in outputs]
for each_item in items:
    if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
        print("Successful: GCP upload successful for file = {}".format(each_item))
    else:
        print("Failed: GCP upload failed for file = {}".format(each_item))
