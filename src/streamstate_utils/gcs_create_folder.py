from google.cloud import storage
from streamstate_utils.pyspark_utils import get_folder_location


def create_gcs_folder(bucket_name: str, app_name: str, topic: str):
    gcs_client = storage.Client()  # optinoally include project
    bucket = gcs_client.get_bucket(bucket_name)
    folder_name = get_folder_location(app_name, topic)
    folder_name_with_delimiter = f"{folder_name}/"
    result = 0
    for blob in gcs_client.list_blobs(bucket_name, prefix=folder_name, delimiter="/"):
        result = 1

    if result == 0:
        blob = bucket.blob(folder_name_with_delimiter)
        blob.upload_from_string(
            "", content_type="application/x-www-form-urlencoded;charset=UTF-8"
        )
