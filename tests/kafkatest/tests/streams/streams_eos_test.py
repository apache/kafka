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

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsEosTestDriverService, StreamsEosTestJobRunnerService, \
    StreamsComplexEosTestJobRunnerService, StreamsEosTestVerifyRunnerService, StreamsComplexEosTestVerifyRunnerService

class StreamsEosTest(KafkaTest):
    """
    Test of Kafka Streams exactly-once semantics
    """

    def __init__(self, test_context):
        super(StreamsEosTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'data': {'partitions': 5, 'replication-factor': 2},
            'echo': {'partitions': 5, 'replication-factor': 2},
            'min': {'partitions': 5, 'replication-factor': 2},
            'sum': {'partitions': 5, 'replication-factor': 2},
            'repartition': {'partitions': 5, 'replication-factor': 2},
            'max': {'partitions': 5, 'replication-factor': 2},
            'cnt': {'partitions': 5, 'replication-factor': 2}
        })
        self.driver = StreamsEosTestDriverService(test_context, self.kafka)
        self.test_context = test_context

    @cluster(num_nodes=9)
    @parametrize(processing_guarantee="exactly_once")
    @parametrize(processing_guarantee="exactly_once_v2")
    def test_rebalance_simple(self, processing_guarantee):
        self.run_rebalance(StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @cluster(num_nodes=9)
    @parametrize(processing_guarantee="exactly_once")
    @parametrize(processing_guarantee="exactly_once_v2")
    def test_rebalance_complex(self, processing_guarantee):
        self.run_rebalance(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                           StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_rebalance(self, processor1, processor2, processor3, verifier):
        """
        Starts and stops two test clients a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()

        self.add_streams(processor1)
        processor1.clean_node_enabled = False
        self.add_streams2(processor1, processor2)
        self.add_streams3(processor1, processor2, processor3)
        self.stop_streams3(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.stop_streams3(processor1, processor3, processor2)
        self.stop_streams2(processor1, processor3)
        self.stop_streams(processor1)
        processor1.clean_node_enabled = True

        self.driver.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)

    @cluster(num_nodes=9)
    @parametrize(processing_guarantee="exactly_once")
    @parametrize(processing_guarantee="exactly_once_v2")
    def test_failure_and_recovery(self, processing_guarantee):
        self.run_failure_and_recovery(StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @cluster(num_nodes=9)
    @parametrize(processing_guarantee="exactly_once")
    @parametrize(processing_guarantee="exactly_once_v2")
    def test_failure_and_recovery_complex(self, processing_guarantee):
        self.run_failure_and_recovery(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, processing_guarantee),
                                      StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_failure_and_recovery(self, processor1, processor2, processor3, verifier):
        """
        Starts two test clients, then abort (kill -9) and restart them a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()

        self.add_streams(processor1)
        processor1.clean_node_enabled = False
        self.add_streams2(processor1, processor2)
        self.add_streams3(processor1, processor2, processor3)
        self.abort_streams(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.abort_streams(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.abort_streams(processor1, processor3, processor2)
        self.stop_streams2(processor1, processor3)
        self.stop_streams(processor1)
        processor1.clean_node_enabled = True

        self.driver.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)

    def add_streams(self, processor):
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.start()
            self.wait_for_startup(monitor, processor)

    def add_streams2(self, running_processor, processor_to_be_started):
        with running_processor.node.account.monitor_log(running_processor.STDOUT_FILE) as monitor:
            self.add_streams(processor_to_be_started)
            self.wait_for_startup(monitor, running_processor)

    def add_streams3(self, running_processor1, running_processor2, processor_to_be_started):
        with running_processor1.node.account.monitor_log(running_processor1.STDOUT_FILE) as monitor:
            self.add_streams2(running_processor2, processor_to_be_started)
            self.wait_for_startup(monitor, running_processor1)

    def stop_streams(self, processor_to_be_stopped):
        with processor_to_be_stopped.node.account.monitor_log(processor_to_be_stopped.STDOUT_FILE) as monitor2:
            processor_to_be_stopped.stop()
            self.wait_for(monitor2, processor_to_be_stopped, "StateChange: PENDING_SHUTDOWN -> NOT_RUNNING")

    def stop_streams2(self, keep_alive_processor, processor_to_be_stopped):
        with keep_alive_processor.node.account.monitor_log(keep_alive_processor.STDOUT_FILE) as monitor:
            self.stop_streams(processor_to_be_stopped)
            self.wait_for_startup(monitor, keep_alive_processor)

    def stop_streams3(self, keep_alive_processor1, keep_alive_processor2, processor_to_be_stopped):
        with keep_alive_processor1.node.account.monitor_log(keep_alive_processor1.STDOUT_FILE) as monitor:
            self.stop_streams2(keep_alive_processor2, processor_to_be_stopped)
            self.wait_for_startup(monitor, keep_alive_processor1)

    def abort_streams(self, keep_alive_processor1, keep_alive_processor2, processor_to_be_aborted):
        with keep_alive_processor1.node.account.monitor_log(keep_alive_processor1.STDOUT_FILE) as monitor1:
            with keep_alive_processor2.node.account.monitor_log(keep_alive_processor2.STDOUT_FILE) as monitor2:
                processor_to_be_aborted.stop_nodes(False)
            self.wait_for_startup(monitor2, keep_alive_processor2)
        self.wait_for_startup(monitor1, keep_alive_processor1)

    def wait_for_startup(self, monitor, processor):
        self.wait_for(monitor, processor, "StateChange: REBALANCING -> RUNNING")
        self.wait_for(monitor, processor, "processed [0-9]* records from topic")

    def wait_for(self, monitor, processor, output):
        monitor.wait_until(output,
                           timeout_sec=480,
                           err_msg=("Never saw output '%s' on " % output) + str(processor.node.account))
