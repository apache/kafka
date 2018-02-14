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

import time
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsBrokerDownResilienceService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService


class StreamsBrokerDownResilience(Test):
    """
    This test validates that Streams is resilient to a broker
    being down longer than specified timeouts in configs
    """

    inputTopic = "streamsResilienceSource"
    outputTopic = "streamsResilienceSink"
    num_messages = 5

    def __init__(self, test_context):
        super(StreamsBrokerDownResilience, self).__init__(test_context=test_context)
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=1,
                                  zk=self.zk,
                                  topics={
                                      self.inputTopic: {'partitions': 3, 'replication-factor': 1},
                                      self.outputTopic: {'partitions': 1, 'replication-factor': 1}
                                  })

    def get_consumer(self, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  self.outputTopic,
                                  "stream-broker-resilience-verify-consumer",
                                  max_messages=num_messages)

    def get_producer(self, num_messages):
        return VerifiableProducer(self.test_context,
                                  1,
                                  self.kafka,
                                  self.inputTopic,
                                  max_messages=num_messages,
                                  acks=1)

    def assert_produce_consume(self, test_state, num_messages=5):
        producer = self.get_producer(num_messages)
        producer.start()

        wait_until(lambda: producer.num_acked >= num_messages,
                   timeout_sec=30,
                   err_msg="At %s failed to send messages " % test_state)

        consumer = self.get_consumer(num_messages)
        consumer.start()

        wait_until(lambda: consumer.total_consumed() >= num_messages,
                   timeout_sec=60,
                   err_msg="At %s streams did not process messages in 60 seconds " % test_state)

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
        result = processor.node.account.ssh_output("grep '%s' %s | wc -l" % (message, file), allow_fail=False)
        return int(result)


    def setUp(self):
        self.zk.start()

    def test_streams_resilient_to_broker_down(self):
        self.kafka.start()

        # Broker should be down over 2x of retries * timeout ms
        # So with (2 * 15000) = 30 seconds, we'll set downtime to 70 seconds
        broker_down_time_in_seconds = 70

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, self.get_configs())
        processor.start()

        # until KIP-91 is merged we'll only send 5 messages to assert Kafka Streams is running before taking the broker down
        # After KIP-91 is merged we'll continue to send messages the duration of the test
        self.assert_produce_consume("before_broker_stop")

        node = self.kafka.leader(self.inputTopic)

        self.kafka.stop_node(node)

        time.sleep(broker_down_time_in_seconds)

        self.kafka.start_node(node)

        self.assert_produce_consume("after_broker_stop")

        self.kafka.stop()

    def test_streams_runs_with_broker_down_initially(self):
        self.kafka.start()
        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        configs = self.get_configs(extra_configs=",application.id=starting_wo_broker_id")

        # start streams with broker down initially
        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        broker_unavailable_message = "Broker may not be available"

        # verify streams instances unable to connect to broker, kept trying
        self.wait_for_verification(processor, broker_unavailable_message, processor.LOG_FILE, 100)
        self.wait_for_verification(processor_2, broker_unavailable_message, processor_2.LOG_FILE, 100)
        self.wait_for_verification(processor_3, broker_unavailable_message, processor_3.LOG_FILE, 100)

        # now start broker
        self.kafka.start_node(node)

        # assert streams can process when starting with broker down
        self.assert_produce_consume("running_with_broker_down_initially", num_messages=9)

        message = "processed3messages"
        # need to show all 3 instances processed messages
        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        self.kafka.stop()

    def test_streams_should_scale_in_while_brokers_down(self):
        self.kafka.start()

        configs = self.get_configs(extra_configs=",application.id=shutdown_with_broker_down")

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        # need to wait for rebalance once
        self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

        # assert streams can process when starting with broker up
        self.assert_produce_consume("waiting for rebalance to complete", num_messages=9)

        message = "processed3messages"

        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.stop()
        processor_2.stop()

        shutdown_message = "Complete shutdown of streams resilience test app now"
        self.wait_for_verification(processor, shutdown_message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, shutdown_message, processor_2.STDOUT_FILE)

        self.kafka.start_node(node)

        self.assert_produce_consume("sending_message_after_stopping_streams_instance_bouncing_broker", num_messages=9)

        self.wait_for_verification(processor_3, "processed9messages", processor_3.STDOUT_FILE)

        self.kafka.stop()

    def test_streams_should_failover_while_brokers_down(self):
        self.kafka.start()

        configs = self.get_configs(extra_configs=",application.id=failover_with_broker_down")

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        # need to wait for rebalance once
        self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

        # assert streams can process when starting with broker up
        self.assert_produce_consume("waiting for rebalance to complete", num_messages=9)

        message = "processed3messages"

        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.abortThenRestart()
        processor_2.abortThenRestart()
        processor_3.abortThenRestart()

        self.kafka.start_node(node)

        self.assert_produce_consume("sending_message_after_hard_bouncing_streams_instance_bouncing_broker", num_messages=9)

        self.kafka.stop()
