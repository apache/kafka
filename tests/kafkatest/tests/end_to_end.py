# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.kafka import TopicPartition
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.utils import validate_delivery

class EndToEndTest(Test):
    """This class provides a shared template for tests which follow the common pattern of:

        - produce to a topic in the background
        - consume from that topic in the background
        - run some logic, e.g. fail topic leader etc.
        - perform validation
    """

    DEFAULT_TOPIC_CONFIG = {"partitions": 2, "replication-factor": 1}

    def __init__(self, test_context, topic="test_topic", topic_config=DEFAULT_TOPIC_CONFIG):
        super(EndToEndTest, self).__init__(test_context=test_context)
        self.topic = topic
        self.topic_config = topic_config
        self.records_consumed = []
        self.last_consumed_offsets = {}
        
    def create_zookeeper_if_necessary(self, num_nodes=1, **kwargs):
        self.zk = ZookeeperService(self.test_context, num_nodes=num_nodes, **kwargs) if quorum.for_test(self.test_context) == quorum.zk else None

    def create_kafka(self, num_nodes=1, **kwargs):
        group_metadata_config = {
            "partitions": num_nodes,
            "replication-factor": min(num_nodes, 3),
            "configs": {"cleanup.policy": "compact"}
        }

        topics = {
            self.topic: self.topic_config,
            "__consumer_offsets": group_metadata_config
        }

        if self.topic:
            topics[self.topic] = self.topic_config

        self.kafka = KafkaService(self.test_context, num_nodes=num_nodes,
                                  zk=self.zk, topics=topics, **kwargs)

    def create_consumer(self, num_nodes=1, group_id="test_group", **kwargs):
        self.consumer = VerifiableConsumer(self.test_context,
                                           num_nodes=num_nodes,
                                           kafka=self.kafka,
                                           topic=self.topic,
                                           group_id=group_id,
                                           on_record_consumed=self.on_record_consumed,
                                           **kwargs)
                                    

    def create_producer(self, num_nodes=1, throughput=1000, **kwargs):
        self.producer = VerifiableProducer(self.test_context,
                                           num_nodes=num_nodes,
                                           kafka=self.kafka,
                                           topic=self.topic,
                                           throughput=throughput,
                                           **kwargs)

    def on_record_consumed(self, record, node):
        partition = TopicPartition(record["topic"], record["partition"])
        record_id = int(record["value"])
        offset = record["offset"]
        self.last_consumed_offsets[partition] = offset
        self.records_consumed.append(record_id)

    def await_produced_records(self, min_records, timeout_sec=30):
        wait_until(lambda: self.producer.num_acked > min_records,
                   timeout_sec=timeout_sec,
                   err_msg="Producer failed to produce messages for %ds." %\
                   timeout_sec)

    def await_consumed_offsets(self, last_acked_offsets, timeout_sec=30):
        def has_finished_consuming():
            for partition, offset in last_acked_offsets.items():
                if not partition in self.last_consumed_offsets:
                    return False
                last_commit = self.consumer.last_commit(partition)
                if not last_commit or last_commit < offset:
                    return False
            return True

        wait_until(has_finished_consuming,
                   timeout_sec=timeout_sec,
                   err_msg="Consumer failed to consume up to offsets %s after waiting %ds." %\
                   (str(last_acked_offsets), timeout_sec))

    def await_consumed_records(self, min_records, producer_timeout_sec=30,
                               consumer_timeout_sec=30):
        self.await_produced_records(min_records=min_records)
        self.await_consumed_offsets(self.producer.last_acked_offsets)

    def _collect_all_logs(self):
        for s in self.test_context.services:
            self.mark_for_collect(s)

    def await_startup(self, min_records=5, timeout_sec=30):
        try:
            wait_until(lambda: self.consumer.total_consumed() >= min_records,
                       timeout_sec=timeout_sec,
                       err_msg="Timed out after %ds while awaiting initial record delivery of %d records" %\
                       (timeout_sec, min_records))
        except BaseException:
            self._collect_all_logs()
            raise

    def run_validation(self, min_records=5000, producer_timeout_sec=30,
                       consumer_timeout_sec=30, enable_idempotence=False):
        try:
            self.await_produced_records(min_records, producer_timeout_sec)
            self.logger.info("Stopping producer after writing up to offsets %s" %\
                         str(self.producer.last_acked_offsets))
            self.producer.stop()

            self.await_consumed_offsets(self.producer.last_acked_offsets, consumer_timeout_sec)
            self.consumer.stop()
            
            self.validate(enable_idempotence)
        except BaseException:
            self._collect_all_logs()
            raise

    def validate(self, enable_idempotence):
        self.logger.info("Number of acked records: %d" % len(self.producer.acked))
        self.logger.info("Number of consumed records: %d" % len(self.records_consumed))

        def check_lost_data(missing_records):
            return self.kafka.search_data_files(self.topic, missing_records)

        succeeded, error_msg = validate_delivery(self.producer.acked, self.records_consumed,
                                                 enable_idempotence, check_lost_data)

        # Collect all logs if validation fails
        if not succeeded:
            self._collect_all_logs()

        assert succeeded, error_msg
