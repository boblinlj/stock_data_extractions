from modules.extract_finviz_data import Finviz
from modules.extract_yahoo_stats import YahooStats
from util.create_output_sqls import write_insert_db
import datetime
from util.gcp_functions import upload_to_bucket
from configs import job_configs as jcfg
import os
from util.helper_functions import create_log

loggerFileName = f"yahoo_stats_{datetime.date.today().strftime('%Y%m%d')}.log"

create_log(loggerName='daily_job',
           loggerFileName=loggerFileName)

# runtime = datetime.datetime.today().date() - datetime.timedelta(days = 3)
runtime = datetime.datetime.today().date()
print(runtime)
print("*" * 30)
print("Extracting Finviz")
# finviz = Finviz(runtime, loggerFileName=loggerFileName)
# finviz.run()
print("*" * 30)
print("Extracting Yahoo Statistics")
spider2 = YahooStats(runtime, loggerFileName=loggerFileName)
spider2.run()
print("*" * 30)

# generate sql script for upload
print("Start to generating output files")
print("*" * 20)
insert = write_insert_db('finviz_screener', runtime)
insert.run_insert()
insert = write_insert_db('finviz_tickers', runtime)
insert.run_insert()
insert = write_insert_db('yahoo_fundamental', runtime)
insert.run_insert()
update = write_insert_db('yahoo_price', runtime)
update.run_insert()
update = write_insert_db('yahoo_consensus', runtime)
update.run_insert()

print("Start Uploading Files to GCP")
items = os.listdir(os.path.join(jcfg.JOB_ROOT, "sql_outputs"))
for each_item in items:
    if each_item.endswith("{}.sql".format(runtime)):
        if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
            print("GCP upload successful for file = {}".format(each_item))
        else:
            print("!!!!!!!!!GCP upload failed for file = {}".format(each_item))
