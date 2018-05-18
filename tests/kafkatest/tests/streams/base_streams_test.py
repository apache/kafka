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

from ducktape.utils.util import wait_until
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.kafka_test import KafkaTest


class BaseStreamsTest(KafkaTest):
    """
    Helper class that contains methods for producing and consuming
    messages and verification of results from log files

    Extends KafkaTest which manages setting up Kafka Cluster and Zookeeper
    see tests/kafkatest/tests/kafka_test.py for more info
    """
    def __init__(self, test_context,  topics, num_zk=1, num_brokers=3):
        super(BaseStreamsTest, self).__init__(test_context, num_zk, num_brokers, topics)

    def get_consumer(self, client_id, topic, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  client_id,
                                  max_messages=num_messages)

    def get_producer(self, topic, num_messages, repeating_keys=None):
        return VerifiableProducer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  max_messages=num_messages,
                                  acks=1,
                                  repeating_keys=repeating_keys)

    def assert_produce_consume(self,
                               streams_source_topic,
                               streams_sink_topic,
                               client_id,
                               test_state,
                               num_messages=5,
                               timeout_sec=60):

        self.assert_produce(streams_source_topic, test_state, num_messages, timeout_sec)

        self.assert_consume(client_id, test_state, streams_sink_topic, num_messages, timeout_sec)

    def assert_produce(self, topic, test_state, num_messages=5, timeout_sec=60):
        producer = self.get_producer(topic, num_messages)
        producer.start()

        wait_until(lambda: producer.num_acked >= num_messages,
                   timeout_sec=timeout_sec,
                   err_msg="At %s failed to send messages " % test_state)

    def assert_consume(self, client_id, test_state, topic, num_messages=5, timeout_sec=60):
        consumer = self.get_consumer(client_id, topic, num_messages)
        consumer.start()

        wait_until(lambda: consumer.total_consumed() >= num_messages,
                   timeout_sec=timeout_sec,
                   err_msg="At %s streams did not process messages in %s seconds " % (test_state, timeout_sec))

    @staticmethod
    def get_configs(extra_configs=""):
        # Consumer max.poll.interval > min(max.block.ms, ((retries + 1) * request.timeout)
        consumer_poll_ms = "consumer.max.poll.interval.ms=50000"
        retries_config = "producer.retries=2"
        request_timeout = "producer.request.timeout.ms=15000"
        max_block_ms = "producer.max.block.ms=30000"

        # java code expects configs in key=value,key=value format
        updated_configs = consumer_poll_ms + "," + retries_config + "," + request_timeout + "," + max_block_ms + extra_configs

        return updated_configs

    def wait_for_verification(self, processor, message, file, num_lines=1):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=60,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    @staticmethod
    def verify_from_file(processor, message, file):
        result = processor.node.account.ssh_output("grep -E '%s' %s | wc -l" % (message, file), allow_fail=False)
        return int(result)

