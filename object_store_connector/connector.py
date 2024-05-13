import datetime
import time
import json
from typing import Any, Dict, Iterator
from obsrv.common import ObsrvException
from obsrv.connector.batch import ISourceConnector
from obsrv.connector import ConnectorContext
from obsrv.connector import MetricsCollector
from obsrv.models import ErrorData, StatusCode, ExecutionState
from obsrv.utils import LoggerController

from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql.functions import lit
from pyspark.sql.types import *

from provider.s3 import S3
from models.object_info import ObjectInfo

logger = LoggerController(__name__)
class ObjectStoreConnector(ISourceConnector):
    def __init__(self):
        self.provider = None
        self.objects = list()
        self.dedupe_tag = None
        self.success_state = StatusCode.SUCCESS.value
        self.error_state = StatusCode.FAILED.value
        self.running_state = ExecutionState.RUNNING.value
        self.not_running_state = ExecutionState.NOT_RUNNING.value
        self.queued_state = ExecutionState.QUEUED.value 
        self.data_format = None
        self.tag_update_failed = dict()

    def process(self, sc: SparkSession, ctx: ConnectorContext, connector_config: Dict[Any, Any], metrics_collector: MetricsCollector) -> Iterator[DataFrame]:
        if (ctx.state.get_state("STATUS", default_value=self.not_running_state) == "running"):
            logger.info("Connector is already running. Skipping processing.")
            return 

        ctx.state.put_state("STATUS", self.running_state)
        ctx.state.save_state()

        last_run_time = datetime.datetime.now()
        ctx.state.put_state("last_run_time", last_run_time)
        ctx.state.save_state()
        self.max_retries = connector_config["max_retries"] if "max_retries" in connector_config else 10
        self.objects_to_process = ctx.state.get_state("to_process")
        self._get_provider(connector_config)
        self._get_objects_to_process(ctx, metrics_collector)
        self.data_format = ctx.data_format
        for res in self._process_objects(sc, ctx, metrics_collector):
            yield res
        
        ctx.state.put_state("STATUS", self.not_running_state)
        ctx.state.put_state("last_run_time", last_run_time)
        ctx.state.save_state()

    def get_spark_conf(self, connector_config) -> SparkConf:
        self._get_provider(connector_config)
        return self.provider.get_spark_config(connector_config)

    def _get_provider(self, connector_config: Dict[Any, Any]):
        if connector_config["type"] == "s3":
            self.provider = S3(connector_config)
        else:
            raise ObsrvException(ErrorData("INVALID_PROVIDER", "provider not supported: {}".format(connector_config["type"])))

    def _get_objects_to_process(self, ctx: ConnectorContext, metrics_collector: MetricsCollector) -> None:
        objects = ctx.state.get_state("to_process", list())
        if ctx.building_block is not None and ctx.env is not None:
            self.dedupe_tag = "{}-{}".format(ctx.building_block, ctx.env)
        else:
            raise ObsrvException(ErrorData("INVALID_CONTEXT", "building_block or env not found in context"))
        if not len(objects):
            objects = self.provider.fetch_objects(ctx, metrics_collector)
            objects = self._exclude_processed_objects(ctx, objects)
            metrics_collector.collect("new_objects_discovered", len(objects))
            ctx.state.put_state("to_process", objects)
            ctx.state.save_state()

        self.objects = objects
        ctx.stats.put_stat("num_files_discovered", len(self.objects))  
        ctx.stats.save_stats()

    def _process_objects(self, sc: SparkSession, ctx: ConnectorContext, metrics_collector: MetricsCollector) -> Iterator[DataFrame]:
        num_files_processed = 0
        for i in range(0, len(self.objects)):
            obj = self.objects[i]
            obj["start_processing_time"] = time.time()
            columns = StructType([])
            df = self.provider.read_object(obj.get("location"), sc=sc, metrics_collector=metrics_collector, file_format=self.data_format)

            if df is None:
                obj.num_of_retries += 1
                if obj.num_of_retries < self.max_retries:
                    ctx.state.put_state("to_process", self.objects[i:])
                else:
                    res = self._update_tag(obj=obj, ctx=ctx, status=self.error_state, metrics_collector=metrics_collector)
                    if not(res):
                        ctx.state.put_state("tag_update_failed",self.tag_update_failed.update({obj["location"]: self.error_state}))
                        ObsrvException(ErrorData("TAG_UPDATE_ERROR", f"failed to update tag for object in path: ", obj["location"]))
                yield sc.createDataFrame(data=[], schema=columns)
            else: 
                df = self._append_custom_meta(sc, df, obj)
                obj["download_time"] = time.time()-obj.get("start_processing_time")
                self._update_tag(obj=obj, ctx=ctx, status=self.success_state, metrics_collector=metrics_collector)
                ctx.state.put_state("to_process", self.objects[i+1:])
                ctx.state.save_state()
                num_files_processed += 1
                ctx.stats.put_stat("num_files_processed",num_files_processed)
                obj["end_processing_time"] = time.time()
                yield df
        
        ctx.stats.save_stats()

    def _update_tag(self, obj: ObjectInfo, ctx: ConnectorContext, status: str, metrics_collector: MetricsCollector) -> bool:
        res = self.provider.update_tag(object=obj, tags=[{"key": self.dedupe_tag, "value": status}], metrics_collector=metrics_collector)
        if not(res):
            ctx.state.put_state("tag_update_failed", self.tag_update_failed.update({obj["location"]: status}))
            raise ObsrvException(ErrorData("TAG_UPDATE_ERROR", f"failed to update tag for object in path: ",obj["location"]))
        return res

    def _append_custom_meta(self, sc: SparkSession, df: DataFrame, object: ObjectInfo) -> DataFrame:
        addn_meta = {
            "location": object.get("location"),
            "file_size_kb": object.get("file_size_kb"),
            "download_time": object.get("download_time"),
            "start_processing_time": object.get("start_processing_time"),
            "end_processing_time": object.get("end_processing_time"),
            "file_hash": object.get("file_hash"),
            "num_of_retries": object.get("num_of_retries"),
            "in_time": object.get("in_time")
        }
        df = df.withColumn("_addn_source_meta", lit(json.dumps(addn_meta, default=str)))
        return df

    def _exclude_processed_objects(self, ctx: ConnectorContext, objects):
        to_be_processed = []
        for obj in objects:
            if not any(tag["key"] == self.dedupe_tag for tag in obj.get("tags")):
                if obj["location"] not in self.tag_update_failed.keys():
                    to_be_processed.append(obj)

        return to_be_processed