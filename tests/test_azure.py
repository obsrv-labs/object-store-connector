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

import json


class TestSource(ISourceConnector):
    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> DataFrame:
        df = sc.read.format("json").load("tests/sample_data/nyt_data_100.json.gz")
        print(df.show())
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

        config = yaml.safe_load(open(config_file_path))


        self.assertEqual(os.path.exists(config_file_path), True)

        test_raw_topic = "azure.ingest"
        test_metrics_topic = "azure.metrics"
        
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config["kafka"]["broker-servers"],
            group_id="azure-test-group",
            enable_auto_commit=True,
        )
        
        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        
        all_messages = kafka_consumer.poll(timeout_ms=10000)

        metrics = []
        metric_names={
                'num_api_calls': 0, 
                'new_objects_discovered': 0
            }
        # num_api_calls=[]
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    # print("message of kafka",message.value)
                    # message_val=json.loads(message.value.decode())
                    # metric=message_val['edata']['metric']
                    # print("metric of edata",metric)
                    # if metric:
                    #     print("num_api_calls",metric['num_api_calls'])
                    # else:
                    #     continue
                    # metrics.append(message.value)
                    msg_val=json.loads(message.value.decode())
                    metrics.append(msg_val)
                # print("msg_val",msg_val)
        # print("metrics",metrics)
        for metric in metrics:
            metric_data = metric.get('edata', {}).get('metric', {})  # Get the 'metric' data from the current metric
            for mn, val in metric_names.items():  # Iterate over metric_names dictionary
                if mn in metric_data:  # Check if the current metric has the key 'mn'
                    metric_names[mn] += metric_data[mn]
        print(metric_names)
            # if data['edata']['metric']['num_api_calls']:
            #     print("num_call",data['edata']['metric']['num_api_calls']) 
            # elif data['edata']['metric']['new_objects_discovered']:
            #     print("new_obj",data['edata']['metric']['new_objects_discovered']) 
            # else:
            #     continue
        count=0
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: 200}
        count+=1
        print("count",count)
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: 9}
        count+=1
        print("count",count)
        # assert kafka_consumer.metrics([tmt_consumer])=={tmt_consumer:76}
        # print("sddd",kafka_consumer.metrics([tmt_consumer]))
        