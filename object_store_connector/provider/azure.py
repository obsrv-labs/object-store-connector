
from typing import Dict, List, Any
from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient
from pyspark.sql import DataFrame, SparkSession
from provider.blob_provider import BlobProvider
from models.object_info import ObjectInfo,Tag
from pyspark.conf import SparkConf
from obsrv.job.batch import get_base_conf
from obsrv.connector import MetricsCollector
from obsrv.common import ObsrvException
from obsrv.models import ErrorData
import json




class AzureBlobStorage(BlobProvider):
    def __init__(self, connector_config: str)-> None:
        super().__init__()
        self.config=connector_config
        self.account_name = self.config["credentials"]["account_name"]
        self.account_key = self.config["credentials"]["account_key"]
        self.container_name = self.config["containername"]
        self.blob_endpoint = self.config["blob_endpoint"]
        self.prefix = self.config["prefix"]
        

        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};BlobEndpoint={self.blob_endpoint}" 
        self.container_client = ContainerClient.from_connection_string(self.connection_string,self.container_name)


    def get_spark_config(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        conf.setAppName("ObsrvObjectStoreConnector")
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1")
        conf.set("fs.azure.storage.accountAuthType", "SharedKey")
        conf.set("fs.azure.storage.accountKey", connector_config["credentials"]["account_key"])
        return conf

    
    def fetch_objects(self,metrics_collector: MetricsCollector) -> List[ObjectInfo]:
        
        try:
            objects = self._list_blobs_in_container(metrics_collector=metrics_collector)
            print("objects:",objects)
            objects_info=[]
            if objects==None:
                raise Exception("No objects found")
    
            for obj in objects:
                #print("in",obj['size'])
                blob_client = BlobClient.from_connection_string(
                    conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
                )
                account_name = self.connection_string.split(";")[1].split("=")[-1]
                blob_location = f"wasb://{self.container_name}@storageemulator/{obj}"
                properties = blob_client.get_blob_properties()

                object_info = ObjectInfo(
                file_size_kb=properties.size,
                location= blob_location,
                format=obj.split(".")[-1],
                tags= self.fetch_tags(obj, metrics_collector)
            )
        
                objects_info.append(object_info.to_json())
            print("--------",objects_info)
        except Exception as e:
            print("Exception: 1",e)
            #exit()
        return objects_info


    def read_object(self, object_path: str, sc: SparkSession, metrics_collector: MetricsCollector,file_format: str) -> DataFrame:
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getObject"},
            {"key": "object_path", "value": object_path}
        ]
        print("o------bj path----\n",object_path)
        # file_format = self.connector_config.get("fileFormat", {}).get("type", "jsonl")
        api_calls, errors, records_count = 0, 0, 0

        try:
            if file_format == "jsonl":
                df = sc.read.format("json").load(object_path)
            elif file_format == "json":
                df = sc.read.format("json").option("multiLine", False).load(object_path)
            elif file_format == "json.gz":
                df = sc.read.format("json").option("compression", "gzip").option("multiLine", True).load(object_path)
            elif file_format == "csv":
                df = sc.read.format("csv").option("header", True).load(object_path)
            elif file_format == "parquet":
                df = sc.read.parquet(object_path)
            elif file_format == "csv.gz":
                df = sc.read.format("csv").option("header", True).option("compression", "gzip").load(object_path)
            else:
                raise ObsrvException(ErrorData("UNSUPPORTED_FILE_FORMAT", f"unsupported file format: {file_format}"))
            records_count = df.count()
            api_calls += 1

            metrics_collector.collect({"num_api_calls": api_calls, "num_records": records_count}, addn_labels=labels)
            print("\n")
            print("dataframe",df)
            return df
        except Exception as e:
             print(f"Failed to read data from Blob {e}")


    def _list_blobs_in_container(self,metrics_collector )->List :
        # container_name = self.config['container_name']
        # prefix = self.prefix
        # summaries = []
        # marker = None

        # # metrics
        # api_calls, errors = 0, 0

        # labels = [
        #     {"key": "request_method", "value": "GET"},
        #     {"key": "method_name", "value": "list_blobs"}
        # ]

        # while True:
        #     try:
        #         self.container_client.list_blobs
        #         blobs = self.container_client.list_blobs(container_name, name_starts_with=prefix, marker=marker)
        #         api_calls += 1
        #         summaries.extend(blobs)
        #         if not blobs.next_marker:
        #             break
        #         marker = blobs.next_marker
        #     except Exception as exception:
        #         errors += 1
        #         labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
        #         metrics_collector.collect("num_errors", errors, addn_labels=labels)
        #         raise ObsrvException(ErrorData('AZURE_BLOB_LIST_ERROR', f"Failed to list objects in Azure Blob Storage: {str(exception)}"))

        # return summaries
        
        try:
            container = ContainerClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name
            )
            
            if container.exists():
                print("Container exists")
            else:
                raise Exception("Container does not exist")
                
            blob_list = container.list_blobs()
            print(" ")
            print("Listing blobs...\n")
            blob_metadata = []
            for blob in blob_list:
                blob_metadata.append(blob.name)
            print(blob_metadata, "\n")

            for blob_name in blob_metadata:
                self._get_properties(self.container_name, blob_name)
            if blob_metadata==None:
                raise Exception("NO objs")
            
            return blob_metadata
        except Exception as ex:
            print("Exception: 2", ex)
            #exit()
        
            
        
    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()

    def _get_properties(self, container_name, blob_name):
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=container_name, blob_name=blob_name
            )
            properties = blob_client.get_blob_properties()

            account_name = self.connection_string.split(";")[1].split("=")[-1]
            blob_location = f"wasb://{account_name}/{container_name}/{blob_name}"

            print(f"Blob location: {blob_location}")
            print(f"Blob name: {blob_name.split('.')[0]}")
            print(f"File Type: {blob_name.split('.')[-1]}")
            print(f"Container: {properties.container}")
            print(f"Created Time:{properties.creation_time}")
            print(f"Blob file_size: {properties.size}")
            print(f"Blob metadata: {properties.metadata}")
            print(f"Blob etag: {properties.etag}")
            print(" ")

        except Exception as ex:
            print("Exception:3", ex)


    def fetch_tags(self, object_path: str, metrics_collector: MetricsCollector) -> List[Tag]:
        obj=object_path.split("//")[-1].split("/")[-1] #
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getObjectTagging"},
            {"key": "object_path", "value": object_path}
        ]

        api_calls, errors = 0, 0
        print("obj :",obj)
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
            )
            tags = blob_client.get_blob_tags()
            print("Blob tags:")
            api_calls += 1
            metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
            for k, v in tags.items():
                print(k, v)
            print("\n")  
            return [Tag(key,value) for key,value in tags.items()]
            
        # except (ValueError, IOError) as e:
        #     print(f"Error retrieving tags: {e}")
        except Exception as exception:
            errors += 1
            labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            raise ObsrvException(ErrorData("AZURE_TAG_READ_ERROR", f"failed to fetch tags from AZURE: {str(exception)}"))
            #print("Exception:4", exception)
        
    

    def update_tag(self,object: ObjectInfo, tags: list, metrics_collector: MetricsCollector) -> bool:
        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "setObjectTagging"},
            {"key": "object_path", "value": object.get('location')}
        ]
        api_calls, errors = 0, 0

        initial_tags = object.get('tags')
        print("::initials tags::\n",initial_tags)
        new_tags = list(json.loads(t) for t in set([json.dumps(t) for t in initial_tags + tags]))
        updated_tags = [Tag(tag.get('key'), tag.get('value')).to_azure() for tag in new_tags]
        try:
            values = list(object.values())
            obj = values[3].split('//')[-1].split('/')[-1]
            
            print("jsdjab",obj)
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
            )
            existing_tags = blob_client.get_blob_tags() or {}
            existing_tags.update(tags)
            blob_client.set_blob_tags(existing_tags)
            print("Blob tags updated!")
            metrics_collector.collect("num_api_calls", api_calls, addn_labels=labels)
            return True
        except (ValueError, IOError) as e:
            print(f"Error setting tags: {e}")
        except Exception as exception:
            errors += 1
            labels += [{"key": "error_code", "value": str(exception.response['Error']['Code'])}]
            metrics_collector.collect("num_errors", errors, addn_labels=labels)
            raise ObsrvException(ErrorData("AZURE_TAG_UPDATE_ERROR", f"failed to update tags in AZURE: {str(exception)}"))

            #print("Exception:5", ex)
        
        print("updated tags",updated_tags)

