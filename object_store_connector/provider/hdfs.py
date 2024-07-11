import json
from typing import Dict, List
import subprocess
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
        spark=self._get_spark_session()
        self.fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        self.path = spark._jvm.org.apache.hadoop.fs.Path(connector_config["source"]["directory_path"])
        

    def get_spark_config(self) -> SparkConf:
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
    
    def fetch_tags(self):
        pass

    def update_tag(self):
        pass

    def list_objects(self, ctx: ConnectorContext) -> List[str]:
        path=self.path
        fs=self.fs
        status = fs.listStatus(path)
        files=[]
        for file_status in status:
         file_path = file_status.getPath()
        if fs.isFile(file_path):
            files.append(file_path.toString())
        else:
            self.list_objects(ctx,file_path)
        return files
        

    def fetch_objects(self,ctx: ConnectorContext) -> List[ObjectInfo]:
        path=self.path
        fs=self.fs
        status=fs.listStatus(path)
        objects_info=[]
        for obj in status:
            if fs.isDirectory(obj.getPath()):
                self.fetch_objects(ctx,obj.getPath())
            else:
             object_info = ObjectInfo(
                location=obj.getPath().toString(),
                format=obj.getPath().toString().split(".")[-1],
                file_size_kb=obj.getLen(),
                file_hash=obj.hashCode()
            )
             objects_info.append(object_info.to_json())
        return objects_info


    def read_object( spark: SparkSession, path: str,file_format: str) -> DataFrame:
      try:
          if file_format.lower() == "csv":
              df = spark.read.format("csv").option("header", True).load(path)
          elif file_format == "json":
              df = spark.read.format("json").option("multiLine", True).load(path)
          elif file_format == "parquet":
              df = spark.read.format("parquet").load(path)
          else:
              print("invalid format")
              return None
          return df
      except Exception as e:
          print(f"Failed to read data from HDFS: {e}")
          return None
      
    def _get_client(self):
         
        pass
        
    
    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()
    
