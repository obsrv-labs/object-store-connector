from google.oauth2 import service_account
from google.cloud import storage
from pyspark.sql import DataFrame
from typing import List
from uuid import uuid4
from pyspark.conf import SparkConf
from models.object_info import Tag,ObjectInfo
from pyspark.sql import SparkSession
from obsrv.connector import MetricsCollector
import json
from obsrv.common import ObsrvException
from obsrv.job.batch import get_base_conf
from google.api_core.exceptions import ClientError
from provider.blob_provider import BlobProvider



class GCS(BlobProvider):
    def __init__(self, connector_config) -> None:
            self.key_path = "/tmp/key.json"
            with open(self.key_path, "w") as f:
                f.write(json.dumps(connector_config["credentials"]))
            super().__init__()
            self.connector_config=connector_config
            self.bucket = connector_config['bucket']
            self.credentials = connector_config['credentials']
            self.prefix = connector_config.get('prefix', '/')
            self.obj_prefix = f"gs://{self.bucket}/"
            self.gcs_client = self._get_client()

    def _get_client(self) -> storage.Client:
        client = storage.Client(
            credentials=service_account.Credentials.from_service_account_info({
                "type": self.credentials["type"],
                "project_id": self.credentials["project_id"],
                "private_key_id": self.credentials["private_key_id"],
                "private_key": self.credentials["private_key"],
                "client_email": self.credentials["client_email"],
                "client_id": self.credentials["client_id"],
                "auth_uri": self.credentials["auth_uri"],
                "token_uri": self.credentials["token_uri"],
                "auth_provider_x509_cert_url": self.credentials["auth_provider_x509_cert_url"],
                "client_x509_cert_url": self.credentials["client_x509_cert_url"]
            })
        )
        try:
            return client
        except Exception as e:
            print(f"Error creating GCS client: {str(e)}")

    def get_spark_config(self,connector_config)->SparkConf:
            conf = get_base_conf()
            conf.setAppName("ObsrvObjectStoreConnector") 
            conf.set("spark.jars", "/root/spark/jars/gcs-connector-hadoop3-2.2.22-shaded.jar") 
            conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", self.key_path) 
            conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true") 
            conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") 
            conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") 
            return conf


    def fetch_tags(self, object_path: str) -> List[Tag]:
        try:
            bucket = self.gcs_client.bucket(self.bucket)
            blob = bucket.blob(object_path)
            if not blob.exists():
                print(f"Object '{object_path}' not found in bucket '{self.bucket}'.")
                return []
            # Fetch metadata from the object
            blob.reload()
            metadata = blob.metadata
            if metadata:
                # return [Tag(tag['Key'], tag['Value']) for tag in tags]
                metadata_list = [Tag(key, value) for key, value in metadata.items()]

                print(f"Metadata fetched for object '{object_path}': {metadata_list}")
                return metadata_list
            else:
                print(f"No metadata found for object '{object_path}'.")
                return []
        except Exception as e:
            print(f"Error fetching metadata: {str(e)}")
            return []


    def update_tag(self, object: ObjectInfo, tags: List[Tag],metrics_collector:MetricsCollector) -> bool:
        bucket=self.connector_config['bucket']
        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "setObjectTagging"},
            {"key": "object_path", "value": object.get('location')}
        ]
        api_calls=0
        try:
            blob = self.gcs_client.bucket(object)
            # Check if the object exists
            if not blob.exists():
                print(f"Object '{object}' not found in bucket '{bucket}'.")
                return False
            # Fetch existing metadata
            blob.reload()
            existing_metadata = blob.metadata
            # Check if the key already exists in the existing metadata
            for item in tags:
                key = item["key"]
                value = item["value"]
                if key in existing_metadata:
                    print(f"Key '{key}' already exists in existing metadata for object '{object}'.")
                    return False
                existing_metadata[key] = value
            # Update metadata for the object
            blob.metadata = existing_metadata
            blob.patch()
            print(f"Metadata updated for object '{object}'.")
            return True
        except Exception as e:
            metrics_collector.collect("num_errors", addn_labels=labels)
            print(f"Error updating metadata: {str(e)}")
            metrics_collector.collect("num_api_calls",api_calls)
            return False
     

  
    def fetch_objects(self,metrics_collector: MetricsCollector) -> List[ObjectInfo]:
        try:
            objects = self._list_objects(metrics_collector=metrics_collector)
            objects_info = []
            if not objects:
                print(f"No objects found")
                return []
            objects_info = []
            for obj in objects:
                # print(obj.name)
                object_info = ObjectInfo(
                    id=str(uuid4()),
                    location=f"{self.obj_prefix}{obj.name}",  # Accessing blob's name
                    # format=obj['name'].split(".")[-1],  # Extracting format from the name
                    file_size_kb=obj.size // 1024,
                    file_hash=obj.etag.strip('"')
                    # tags=self.gcs_client.fetch_tags(obj['Key'], metrics_collector)
                )
            print(f"{self.obj_prefix}")
            objects_info.append(object_info.to_json())
            print(objects_info) 
            return objects_info
        except Exception as e:
            print(f"Error fetching objects: {str(e)}")
            return []



    def read_object(self, object_path: str, sc: SparkSession,metrics_collector:MetricsCollector, file_format: str) -> DataFrame:
        labels = [
             {"key": "request_method", "value": "GET"},
             {"key": "method_name", "value": "getObject"},
             {"key": "object_path", "value": object_path}
         ]

        print("In read object", object_path)
        
        api_calls= 0
        try:
             # Determine file format based on the provided parameter
             if file_format == "jsonl":
                 df = sc.read.format("json").load(object_path)
             elif file_format == "json":
                 df = sc.read.format("json").option("multiLine", True).load(object_path)
             elif file_format == "json.gz":
                 df = sc.read.format("json").option("compression", "gzip").option("multiLine", True).load(object_path)
             elif file_format == "csv":
                 df = sc.read.format("csv").option("header", True).load(object_path)
             elif file_format == "csv.gz":
                 df = sc.read.format("csv").option("header", True).option("compression", "gzip").load(object_path)
             else:
               raise Exception(f"Unsupported file format for file: {file_format}")
            #  records_count = df.count()
             api_calls += 1
             metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
             return df 
        except Exception as exception:
            metrics_collector.collect("num_errors", addn_labels=labels)
            raise Exception(f"failed to read object from GCS: {str(exception)}")



    
    def _list_objects(self,metrics_collector:MetricsCollector):
        """Lists all the blobs in the bucket."""
        bucket_name = self.connector_config['bucket']
        api_calls=0
        summaries=[]
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "ListBlobs"}
        ]
        try:
            # Note: Client.list_blobs requires at least package version 1.17.0.
            objects = self.gcs_client.list_blobs(bucket_name)
            # Note: The call returns a response only when the iterator is consumed.
            for obj in objects:
                # print(blob.name)
                summaries.append(obj)
        except ( ClientError) as exception:
                # errors += 1
                labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
                metrics_collector.collect("num_errors", addn_labels=labels)
                raise Exception(f"failed to list objects in GCS: {str(exception)}")
        metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
        # print(summaries)
        return summaries

    def _get_spark_session(self):
             return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()

