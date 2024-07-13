from typing import Dict, List
from models.object_info import ObjectInfo
from provider.blob_provider import BlobProvider
from pyspark.conf import SparkConf
from obsrv.job.batch import get_base_conf
from pyspark.sql import DataFrame,SparkSession
from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector

class HDFS(BlobProvider):
    def __init__(self, connector_config) -> None:
        super().__init__()
        self.connector_config = connector_config
       
    def get_spark_config(self,connector_config) -> SparkConf:
        conf = get_base_conf()
        conf.setAppName("ObsrvObjectStoreConnector") 
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
        conf.set(
            "spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"
        )  # Use HDFS file system
        conf.set(
            "spark.hadoop.security.authentication","kerberos"
        )  # Set kerberos authentication
        conf.set(
            "spark.hadoop.security.enabled", "true"
        )  
        conf.set(
            "spark.hadoop.kerberos.krb5.path","/etc/krb5.conf"
        )  # Path to kerberos configuration 
        conf.set(
            "spark.hadoop.dfs.namenode.kerberos.principal","hdfs/namenode@EXAMPLE.COM"
        )  # Set kerberos namenode principal
    
        return conf
    
    def fetch_tags(self, object_path: str, metrics_collector: MetricsCollector):
        pass

    def update_tag(self, object: ObjectInfo, tags: list, metrics_collector: MetricsCollector):
        pass

    def list_objects(self) -> List[str]:
        # self.spark=self._get_spark_session()
        # self.fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        # self.path = self.spark._jvm.org.apache.hadoop.fs.Path(self.connector_config["source"]["directory_path"])

        # status = self.fs.listStatus(self.path)
        # files=[]
        # for file_status in status:
        #  file_path = file_status.getPath()
        # if self.fs.isFile(file_path):
        #     files.append(file_path.toString())
        # else:
        #     self.path=file_path
        #     self.list_objects(self,ctx)
        # return files
        pass

    def fetch_objects(self,ctx: ConnectorContext,metrics_collector:MetricsCollector)-> List[ObjectInfo]:
        self.spark=self._get_spark_session()
        self.fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        self.path =self.spark._jvm.org.apache.hadoop.fs.Path("hdfs:/")

        status=self.fs.listStatus(self.path)
        objects_info=[]
        for obj in status:
            if self.fs.isDirectory(obj.getPath()):
                self.path=obj.getPath()
                self.fetch_objects(self,ctx,metrics_collector)
            else:
             object_info = ObjectInfo(
                location=obj.getPath().toString(),
                format=obj.getPath().toString().split(".")[-1],
                file_size_kb=obj.getLen(),
                file_hash=obj.hashCode()
            )
             objects_info.append(object_info.to_json())
        print(objects_info)
        return objects_info
        


    def read_object(self,path: str, sc: SparkSession,metrics_collector:MetricsCollector,file_format: str) -> DataFrame:
      labels = [
      {"key": "request_method", "value": "GET"},
      {"key": "method_name", "value": "getObject"},
      {"key": "object_path", "value": path},
      ]
      api_calls, errors, records_count = 0, 0, 0
      try:
       
         if file_format == "jsonl":
                df = sc.read.format("json").load(path)
         elif file_format == "json":
                df = sc.read.format("json").option("multiLine", True).load(path)
         elif file_format == "csv":
                df = sc.read.format("csv").option("header", True).load(path)
         elif file_format == "parquet":
                df = sc.read.format("parquet").load(path)

         else:
              print("invalid format")
              return None
         records_count = df.count()
         api_calls += 1
         metrics_collector.collect(
                {"num_api_calls": api_calls, "num_records": records_count},
                addn_labels=labels,
         )
         df.show()
         return df
           
      except Exception as e:
          print(f"Failed to read data from HDFS: {e}")
          return None
               
    def _get_client(self):
         
        pass
        
    
    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config(self.connector_config)).getOrCreate()
    
