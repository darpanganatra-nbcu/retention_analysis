from google.cloud import storage
import glob
import os

def upload_blob(input_file):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "INSERT_CREDENTIALS_HERE"
    storage_client = storage.Client()
    bucket = storage_client.bucket('retention-analysis')
    file_list = [x.split("/")[-1] for x in glob.glob(f"""{input_file}/*.csv""")]
    for file in file_list:
        blob = bucket.blob(f"""{file}""")
        blob.upload_from_filename(f"Data/{file}")
