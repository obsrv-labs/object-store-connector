from google.oauth2 import service_account
from google.cloud import storage
from pyspark.sql import DataFrame
from typing import List
from uuid import uuid4
from pyspark.conf import SparkConf
from models.object_info import Tag,ObjectInfo
from pyspark.sql import SparkSession
from obsrv.connector import MetricsCollector
from obsrv.job.batch import get_base_conf
from google.api_core.exceptions import ClientError
from provider.blob_provider import BlobProvider
import json

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
            conf.set("spark.sql.debug.maxToStringFields", 1000) 
            return conf


    def fetch_tags(self, object_path: str, metrics_collector: MetricsCollector) -> List[Tag]:
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getObjectMetadata"},
            {"key": "object_path", "value": object_path}
        ]
        api_calls, errors = 0, 0
        try:
            relative_path = object_path.replace("gs://" + self.bucket + "/", "")
            bucket = self.gcs_client.bucket(self.bucket)
            blob = bucket.get_blob(relative_path)
            if blob is None:
                print(f"Object '{object_path}' not found in bucket '{self.bucket}'.")
                return []
            api_calls += 1
            # Fetch metadata from the object
            blob.reload()
            metadata = blob.metadata
            tags = []
            if metadata:
                for key, value in metadata.items():
                    tags.append({'Key': key, 'Value': value})
                metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
                print(f"Tags:{tags}")
        except Exception as exception:
            errors += 1
            labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            raise Exception(f"failed to fetch tags from S3: {str(exception)}")
        return tags


    def update_tag(self, object: ObjectInfo, tags: List[Tag], metrics_collector: MetricsCollector) -> bool:
        object_path = object.get('location')
        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "updateObjectMetadata"},
            {"key": "object_path", "value": object_path}
        ]
        api_calls,errors=0,0
        try:
            initial_tags = object.get('tags')
            print(f"Initial:{initial_tags}")
            new_tags = list(json.loads(t) for t in set([json.dumps(t) for t in initial_tags + tags]))
            print(f"New tags:{new_tags}")
            # Filter out tags with missing key or value
            valid_tags = [tag for tag in new_tags if 'key' in tag and 'value' in tag]
            updated_tags = [Tag(tag['key'], tag['value']).to_gcs() for tag in valid_tags]
            print(f"Updated: {updated_tags}")
            metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
            return True
        except (ClientError) as exception:
            errors += 1
            labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            print(f"Error updating metadata: {str(exception)}")
            raise Exception(f"failed to update tags in GCS: {str(exception)}")

  
    def fetch_objects(self, metrics_collector: MetricsCollector) -> List[ObjectInfo]:
        objects = self._list_objects(metrics_collector=metrics_collector)
        objects_info = []
        if not objects:
            print(f"No objects found")
            return []
        for obj in objects:
            object_info = ObjectInfo(
                id=str(uuid4()),
                location=f"{self.obj_prefix}{obj.name}",  # Accessing blob's name
                format=(obj.name).split(".")[-1],  # Extracting format from the name
                file_size_kb=obj.size // 1024,
                file_hash=obj.etag.strip('"'),
                tags=self.fetch_tags(f"{self.obj_prefix}{obj.name}", metrics_collector)
            )
            objects_info.append(object_info.to_json())
        return objects_info


    def read_object(self, object_path: str, sc: SparkSession,metrics_collector:MetricsCollector, file_format: str) -> DataFrame:
        labels = [
             {"key": "request_method", "value": "GET"},
             {"key": "method_name", "value": "getObject"},
             {"key": "object_path", "value": object_path}
         ]
        api_calls,errors,records_count= 0,0,0
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
             records_count = df.count()
             api_calls += 1
             metrics_collector.collect({"num_api_calls": api_calls, "num_records": records_count}, addn_labels=labels)
             return df
        except (ClientError) as exception:
            errors += 1
            labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            raise Exception( f"failed to read object from S3: {str(exception)}")
        except Exception as exception:
            errors += 1
            labels += [{"key": "error_code", "value": "S3_READ_ERROR"}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            raise Exception( f"failed to read object from S3: {str(exception)}")

    
    def _list_objects(self,metrics_collector:MetricsCollector):
        bucket_name = self.connector_config['bucket']
        summaries=[]
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "ListBlobs"}
        ]
        api_calls,errors=0,0
        try:
            # Note: Client.list_blobs requires at least package version 1.17.0.
            objects = self.gcs_client.list_blobs(bucket_name)
            for obj in objects:
                summaries.append(obj)
        except ( ClientError) as exception:
                errors += 1
                labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
                metrics_collector.collect("num_errors", errors, addn_labels=labels)
                raise Exception (f"failed to list objects in GCS: {str(exception)}")
        metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
        return summaries

    def _get_spark_session(self):
             return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()

