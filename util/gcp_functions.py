import os
import sys
from util.create_output_sqls import write_insert_db
from google.cloud import storage
from configs import job_configs as jcfg

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(jcfg.JOB_ROOT, f'inputs/{jcfg.GOOGLE_KEY}')
storage_client = storage.Client()


def upload_to_bucket(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False


def upload_sql_to_GCP_cloud_storage(file_name_list: list, runtime):
    sys.stderr.write("Start to generating output files\n")
    sys.stderr.write(f"{'*'*80}\n")
    for sql_out in file_name_list:
        write_insert_db(sql_out, runtime).run_insert()

    sys.stderr.write("Start Uploading Files to GCP\n")
    items = [f'insert_{file}_{runtime}.sql' for file in file_name_list]
    for each_item in items:
        if upload_to_bucket(each_item, os.path.join(jcfg.JOB_ROOT, "sql_outputs", each_item), 'stock_data_busket2'):
            sys.stderr.write(f"Successful: GCP upload successful for file = {each_item}\n")
        else:
            sys.stderr.write(f"Failed: GCP upload failed for file = {each_item}\n")



if __name__ == '__main__':
    pass
