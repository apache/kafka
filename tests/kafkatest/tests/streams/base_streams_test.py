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
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.kafka import KafkaService


class BaseStreamsTest(Test):
    """
    Helper class that contains methods for producing and consuming
    messages and verification of results from log files

    Extends KafkaTest which manages setting up Kafka Cluster and Zookeeper
    see tests/kafkatest/tests/kafka_test.py for more info
    """
    def __init__(self, test_context, topics, num_controllers=1, num_brokers=3):
        super(BaseStreamsTest, self).__init__(test_context = test_context)
        self.num_controllers = num_controllers
        self.num_brokers = num_brokers
        self.topics = topics
        self.use_streams_groups = True

        self.kafka = KafkaService(
            test_context, self.num_brokers,
            None, topics=self.topics,
            controller_num_nodes_override=self.num_controllers,
            use_streams_groups=True,
            server_prop_overrides=[
                [ "group.streams.min.session.timeout.ms", "10000" ], # Need to up the lower bound
                [ "group.streams.session.timeout.ms", "10000" ] # As in classic groups, set this to 10s
            ]
        )

    def setUp(self):
        self.kafka.start()
        if self.use_streams_groups:
            self.kafka.run_features_command("upgrade", "streams.version", 1)

    def get_consumer(self, client_id, topic, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  client_id,
                                  max_messages=num_messages)

    def get_producer(self, topic, num_messages, throughput=1000, repeating_keys=None):
        return VerifiableProducer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  max_messages=num_messages,
                                  acks=-1,
                                  throughput=throughput,
                                  repeating_keys=repeating_keys,
                                  retries=10)

    def assert_produce_consume(self,
                               streams_source_topic,
                               streams_sink_topic,
                               client_id,
                               test_state,
                               num_messages=5,
                               timeout_sec=60):

        self.assert_produce(streams_source_topic, test_state, num_messages, timeout_sec)

        self.assert_consume(client_id, test_state, streams_sink_topic, num_messages, timeout_sec)

    def assert_produce(self, topic, test_state, num_messages=5, timeout_sec=60, repeating_keys=None):
        producer = self.get_producer(topic, num_messages, repeating_keys=repeating_keys)
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

    def configure_standby_replicas(self, group_id, num_standby_replicas):
        force_use_zk_connection = not self.kafka.all_nodes_configs_command_uses_bootstrap_server()
        node = self.kafka.nodes[0]
        cmd = "%s --alter --add-config streams.num.standby.replicas=%d --entity-type groups --entity-name %s" % \
              (
                    self.kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection),
                    num_standby_replicas,
                    group_id
              )
        node.account.ssh(cmd)

    @staticmethod
    def get_configs(group_protocol="classic", extra_configs=""):
        # Consumer max.poll.interval > min(max.block.ms, ((retries + 1) * request.timeout)
        consumer_poll_ms = "consumer.max.poll.interval.ms=50000"
        retries_config = "producer.retries=2"
        request_timeout = "producer.request.timeout.ms=15000"
        max_block_ms = "producer.max.block.ms=30000"
        group_protocol = "group.protocol=" + group_protocol

        # java code expects configs in key=value,key=value format
        updated_configs = consumer_poll_ms + "," + retries_config + "," + request_timeout + "," + max_block_ms + "," + group_protocol + extra_configs

        return updated_configs

    def wait_for_verification(self, processor, message, file, num_lines=1):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=60,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    def verify_from_file(self, processor, message, file):
        result = processor.node.account.ssh_output("grep -E '%s' %s | wc -l" % (message, file), allow_fail=False)
        try:
          return int(result)
        except ValueError:
          self.logger.warn("Command failed with ValueError: " + str(result, errors='strict'))
          return 0

