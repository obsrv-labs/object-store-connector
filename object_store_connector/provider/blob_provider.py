from abc import ABC, abstractmethod
from typing import Any, Dict, List

from object_store_connector.models.object_info import ObjectInfo, Tag
from obsrv.connector import MetricsCollector
from pyspark.sql import DataFrame, SparkSession


class JobConfig:
    pass


class BlobProvider(ABC):
    @abstractmethod
    def get_spark_config(self, config: JobConfig) -> Dict[str, Any]:
        pass

    @abstractmethod
    def fetch_tags(
        self,
        objectPath: str,
        metrics_collector: MetricsCollector,
        config: Dict[str, Any] = None,
    ) -> List[Tag]:
        pass

    @abstractmethod
    def update_tag(
        self,
        object: ObjectInfo,
        tags: List[Tag],
        metrics_collector: MetricsCollector,
        config: Dict[str, Any] = None,
    ) -> bool:
        pass

    @abstractmethod
    def fetch_objects(
        self,
        metrics_collector: MetricsCollector,
        connectorConfig: Dict[str, Any] = None,
    ) -> List[ObjectInfo]:
        pass

    @abstractmethod
    def read_object(
        self,
        objectPath: str,
        metrics_collector: MetricsCollector,
        config: Dict[str, Any] = None,
        sc: SparkSession = None,
    ) -> DataFrame:
        pass
