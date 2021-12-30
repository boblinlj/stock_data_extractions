import datetime
from util.gcp_functions import upload_to_bucket
from configs import job_configs as jcfg
import os
from util.helper_functions import create_log
from util.create_output_sqls import write_insert_db
from modules.extract_analysis import YahooAnalysis

loggerFileName = f"Weekly_Yahoo_Analysis_Job_{datetime.date.today().strftime('%Y%m%d')}.log"

create_log(loggerName='Weekly_Yahoo_Analysis_Job', loggerFileName=loggerFileName)

# runtime = datetime.datetime.today().date() - datetime.timedelta(days = 3)
runtime = datetime.datetime.today().date()
print(runtime)
print("*" * 30)
print("Extracting Yahoo Analysis Data")
YahooAnalysis = YahooAnalysis(updated_dt=runtime, upload_to_gcp=False, batch_run=True, loggerFileName=loggerFileName)
YahooAnalysis.run_job()

outputs = ['yahoo_consensus']
# generate sql script for upload
print("Start to generating output files")
print("*" * 30)
for sql_out in outputs:
    write_insert_db(sql_out, runtime).run_insert()


print("Start Uploading Files to GCP")
# items = os.listdir(os.path.join(jcfg.JOB_ROOT, "sql_outputs"))
items = [f'insert_{file}_{runtime}.sql' for file in outputs]
for each_item in items:
    if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
        print("Successful: GCP upload successful for file = {}".format(each_item))
    else:
        print("Failed: GCP upload failed for file = {}".format(each_item))