from google.cloud import storage, bigquery
from typing import List, Dict, Any
import pandas as pd

class GCSClientHelper:
    def __init__(self, project_id: str):
        """
        Initialize the Google Cloud Storage client and Bigquery Client for use with Google Cloud Storage.

        Args:
            project_id (str): The Google Cloud Project ID.
        """
        self.storage_client = storage.Client(project=project_id)
        self.bigquery_client = bigquery.Client(project=project_id)

    def create_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Create a new bucket within Google Cloud Storage in your initialised project.

        Args:
            bucket_name (str): The name of the bucket to create.

        Returns:
            storage.Bucket: The created bucket object.
        """
        bucket = self.storage_client.create_bucket(bucket_name)
        return bucket

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket within Google Cloud Storage in your initialised project.

        Args:
            bucket_name (str): The name of the bucket to delete.

        Returns:
            None
        """
        bucket = self.storage_client.get_bucket(bucket_name)
        bucket.delete()

    def upload_blob(self, bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
        """
        Upload a file to a bucket in your initialised project.

        Args:
            bucket_name (str): The name of the bucket.
            source_file_name (str): The path to the file to upload.
            destination_blob_name (str): The name of the destination blob.

        Returns:
            None
        """
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

    def download_blob(self, bucket_name: str, source_blob_name: str, destination_file_name: str) -> None:
        """
        Download a blob from a bucket within your initialised project to local file storage.

        Args:
            bucket_name (str): The name of the bucket.
            source_blob_name (str): The name of the source blob.
            destination_file_name (str): The path to the file to download.

        Returns:
            None
        """
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

    def list_blobs(self, bucket_name: str) -> List[str]:
        """
        List all blobs in a bucket in initialised project.

        Args:
            bucket_name (str): The name of the bucket.

        Returns:
            List[str]: A list of blob names.
        """
        bucket = self.storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        return [blob.name for blob in blobs]

    def blob_to_bigquery_table(self, bucket_name: str, blob_name: str, table_id: str) -> None:
        """
        Load a blob from Google Cloud Storage into a BigQuery table within initialised project.

        Args:
            bucket_name (str): The name of the bucket.
            blob_name (str): The name of the blob.
            table_id (str): The ID of the BigQuery table.

        Returns:
            None
        """
        uri = f"gs://{bucket_name}/{blob_name}"
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV)
        load_job = self.bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

    def move_blob(self, bucket_name: str, blob_name: str, destination_bucket_name: str, destination_blob_name: str) -> None:
        """
        Move a blob from one bucket to another bucket within initialised project.

        Args:
            bucket_name (str): The name of the source bucket.
            blob_name (str): The name of the source blob.
            destination_bucket_name (str): The name of the destination bucket.
            destination_blob_name (str): The name of the destination blob.

        Returns:
            None
        """
        source_bucket = self.storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = self.storage_client.get_bucket(destination_bucket_name)
        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
        source_blob.delete()
