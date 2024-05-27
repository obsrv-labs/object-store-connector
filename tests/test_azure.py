import os
import unittest
from typing import Any, Dict

import pytest
import yaml
from kafka import KafkaConsumer, TopicPartition
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession

from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector, SourceConnector
from obsrv.job.batch import get_base_conf
from tests.batch_setup import setup_obsrv_database  # noqa

from object_store_connector.connector import ObjectStoreConnector




class ObjectStoreConnector(ISourceConnector):
    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> DataFrame:
        df = sc.read.format("json").load("tests/sample_data/nyt_data_100.json.gz")
        # print(df.show())
        yield df

        df1 = sc.read.format("json").load("tests/sample_data/nyt_data_100.json")

        yield df1

    def get_spark_conf(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        return conf


@pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):

    def test_source_connector(self):

        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")
        # SourceConnector.process(connector=connector, config_file_path=config_file_path)
         ## import the objectstorreconnector class

        ## update the config file with your instances

        ## call SourceConnector.process* - get the logic from __main__py.

        # connector = TestSource()
        # config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        config = yaml.safe_load(open(config_file_path))

        # config['connector_instance_id'] = "azure.new-york-taxi-data.1"

        # with open(
        #     config_file_path, "w"
        # ) as config_file:
        #     yaml.dump(config, config_file)


        self.assertEqual(os.path.exists(config_file_path), True)

        test_raw_topic = "azure.ingest"
        test_metrics_topic = "azure.metrics"
        # import time
        # time.sleep(100)
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config["kafka"]["broker-servers"],
            group_id="azure-test-group",
            enable_auto_commit=True,
        )
        # time.sleep(100)
        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        # kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        print("Done processing....0")

        # import time
        # time.sleep(30)
        all_messages = kafka_consumer.poll(timeout_ms=10000)

        metrics = []
        # for topic_partition, messages in all_messages.items():
        #     for message in messages:
        #         if topic_partition.topic == test_metrics_topic:
        #             metrics.append(message.value)

        # Check number of messages
        print(f"Number of messages in {test_metrics_topic}: {len(metrics)}")
        for message in metrics:
            print(message)
        print("kkk",kafka_consumer.end_offsets([trt_consumer]))
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: 200}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: 1}