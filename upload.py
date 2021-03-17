from google.cloud import storage
bucket_name = "tcs-lbg-demo"

destination_blob_name = "mohan.csv"
source_blob_name = "testdata.csv"
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
newblob = bucket.blob("processed/" +destination_blob_name)
newblob.upload_from_filename(source_blob_name)
print(f"the file is uploaded to the {newblob}")
