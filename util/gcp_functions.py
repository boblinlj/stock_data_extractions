import os
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


if __name__ == '__main__':
    pass
