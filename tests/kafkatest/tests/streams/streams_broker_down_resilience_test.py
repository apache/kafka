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
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.services.kafka import quorum
from kafkatest.services.streams import StreamsBrokerDownResilienceService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest


class StreamsBrokerDownResilience(BaseStreamsTest):
    """
    This test validates that Streams is resilient to a broker
    being down longer than specified timeouts in configs
    """

    inputTopic = "streamsResilienceSource"
    outputTopic = "streamsResilienceSink"
    client_id = "streams-broker-resilience-verify-consumer"
    num_messages = 10000
    message = "processed [0-9]* messages"
    connected_message = "Discovered group coordinator"

    def __init__(self, test_context):
        super(StreamsBrokerDownResilience, self).__init__(test_context,
                                                          topics={self.inputTopic: {'partitions': 3, 'replication-factor': 1},
                                                                  self.outputTopic: {'partitions': 1, 'replication-factor': 1}},
                                                          num_brokers=1)

    def setUp(self):
        if self.zk:
            self.zk.start()

    @cluster(num_nodes=7)
    @matrix(metadata_quorum=[quorum.remote_kraft])
    def test_streams_resilient_to_broker_down(self, metadata_quorum):
        self.kafka.start()

        # Broker should be down over 2x of retries * timeout ms
        # So with (2 * 15000) = 30 seconds, we'll set downtime to 70 seconds
        broker_down_time_in_seconds = 70

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, self.get_configs())
        processor.start()

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "before_broker_stop")

        node = self.kafka.leader(self.inputTopic)

        self.kafka.stop_node(node)

        time.sleep(broker_down_time_in_seconds)

        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            self.kafka.start_node(node)
            monitor.wait_until(self.connected_message,
                               timeout_sec=120,
                               err_msg=("Never saw output '%s' on " % self.connected_message) + str(processor.node.account))

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "after_broker_stop",
                                    timeout_sec=120)

        self.kafka.stop()

    @cluster(num_nodes=7)
    @matrix(metadata_quorum=[quorum.remote_kraft])
    def test_streams_runs_with_broker_down_initially(self, metadata_quorum):
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
        self.wait_for_verification(processor, broker_unavailable_message, processor.LOG_FILE, 10)
        self.wait_for_verification(processor_2, broker_unavailable_message, processor_2.LOG_FILE, 10)
        self.wait_for_verification(processor_3, broker_unavailable_message, processor_3.LOG_FILE, 10)

        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.LOG_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.LOG_FILE) as monitor_3:
                    self.kafka.start_node(node)

                    monitor_1.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor.node.account))
                    monitor_2.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor_3.node.account))

        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.STDOUT_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.STDOUT_FILE) as monitor_3:

                    self.assert_produce(self.inputTopic,
                                        "sending_message_after_broker_down_initially",
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

                    monitor_1.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor.node.account))
                    monitor_2.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_3.node.account))

                    self.assert_consume(self.client_id,
                                        "consuming_message_after_broker_down_initially",
                                        self.outputTopic,
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

        self.kafka.stop()

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.remote_kraft])
    def test_streams_should_scale_in_while_brokers_down(self, metadata_quorum):
        self.kafka.start()

        # TODO KIP-441: consider rewriting the test for HighAvailabilityTaskAssignor
        configs = self.get_configs(
            extra_configs=",application.id=shutdown_with_broker_down" +
                          ",internal.task.assignor.class=org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor"
        )

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)

        # need to wait for rebalance once
        rebalance = "State transition from REBALANCING to RUNNING"
        with processor_3.node.account.monitor_log(processor_3.LOG_FILE) as monitor:
            processor_3.start()

            monitor.wait_until(rebalance,
                               timeout_sec=120,
                               err_msg=("Never saw output '%s' on " % rebalance) + str(processor_3.node.account))

        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.STDOUT_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.STDOUT_FILE) as monitor_3:

                    self.assert_produce(self.inputTopic,
                                        "sending_message_normal_broker_start",
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

                    monitor_1.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor.node.account))
                    monitor_2.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_3.node.account))

                    self.assert_consume(self.client_id,
                                        "consuming_message_normal_broker_start",
                                        self.outputTopic,
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.stop()
        processor_2.stop()

        shutdown_message = "Complete shutdown of streams resilience test app now"
        self.wait_for_verification(processor, shutdown_message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, shutdown_message, processor_2.STDOUT_FILE)

        with processor_3.node.account.monitor_log(processor_3.LOG_FILE) as monitor_3:
            self.kafka.start_node(node)

            monitor_3.wait_until(self.connected_message,
                                 timeout_sec=120,
                                 err_msg=("Never saw '%s' on " % self.connected_message) + str(processor_3.node.account))

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "sending_message_after_stopping_streams_instance_bouncing_broker",
                                    num_messages=self.num_messages,
                                    timeout_sec=120)

        self.kafka.stop()

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.remote_kraft])
    def test_streams_should_failover_while_brokers_down(self, metadata_quorum):
        self.kafka.start()

        # TODO KIP-441: consider rewriting the test for HighAvailabilityTaskAssignor
        configs = self.get_configs(
            extra_configs=",application.id=failover_with_broker_down" +
                          ",internal.task.assignor.class=org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor"
        )

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)

        # need to wait for rebalance once
        rebalance = "State transition from REBALANCING to RUNNING"
        with processor_3.node.account.monitor_log(processor_3.LOG_FILE) as monitor:
            processor_3.start()

            monitor.wait_until(rebalance,
                               timeout_sec=120,
                               err_msg=("Never saw output '%s' on " % rebalance) + str(processor_3.node.account))

        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.STDOUT_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.STDOUT_FILE) as monitor_3:

                    self.assert_produce(self.inputTopic,
                                        "sending_message_after_normal_broker_start",
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

                    monitor_1.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor.node.account))
                    monitor_2.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_3.node.account))

                    self.assert_consume(self.client_id,
                                        "consuming_message_after_normal_broker_start",
                                        self.outputTopic,
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.abortThenRestart()
        processor_2.abortThenRestart()
        processor_3.abortThenRestart()

        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.LOG_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.LOG_FILE) as monitor_3:
                    self.kafka.start_node(node)

                    monitor_1.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor.node.account))
                    monitor_2.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.connected_message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.connected_message) + str(processor_3.node.account))

        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor_1:
            with processor_2.node.account.monitor_log(processor_2.STDOUT_FILE) as monitor_2:
                with processor_3.node.account.monitor_log(processor_3.STDOUT_FILE) as monitor_3:

                    self.assert_produce(self.inputTopic,
                                        "sending_message_after_hard_bouncing_streams_instance_bouncing_broker",
                                        num_messages=self.num_messages,
                                        timeout_sec=120)

                    monitor_1.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor.node.account))
                    monitor_2.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_2.node.account))
                    monitor_3.wait_until(self.message,
                                         timeout_sec=120,
                                         err_msg=("Never saw '%s' on " % self.message) + str(processor_3.node.account))

                    self.assert_consume(self.client_id,
                                        "consuming_message_after_stopping_streams_instance_bouncing_broker",
                                        self.outputTopic,
                                        num_messages=self.num_messages,
                                        timeout_sec=120)
        self.kafka.stop()
