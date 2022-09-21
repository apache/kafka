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

import random
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsEosTestDriverService, StreamsEosTestJobRunnerService, \
    StreamsComplexEosTestJobRunnerService, StreamsEosTestVerifyRunnerService, StreamsComplexEosTestVerifyRunnerService
from kafkatest.version import LATEST_3_0, LATEST_3_1, LATEST_3_2, DEV_VERSION

from kafkatest.tests.streams.utils.util import wait_for, verify_stopped

eos_v1_versions = [str(LATEST_3_0), str(LATEST_3_1), str(LATEST_3_2)]
eos_v2_versions = [str(DEV_VERSION)]

class StreamsEosUpgradeTest(KafkaTest):

    processed_data_msg = "processed [0-9]* records from topic=data"
    base_version_number = str(DEV_VERSION).split("-")[0]

    """
    Test of Kafka Streams exactly-once semantics
    """

    def __init__(self, test_context):
        super(StreamsEosUpgradeTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
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
    @matrix(from_version=eos_v1_versions,
            to_version=eos_v2_versions,
            clean_shutdown=[True, False])
    def test_eos_upgrade_simple(self, from_version, to_version, clean_shutdown):
        self.processor1 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor2 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor3 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.verifier = StreamsEosTestVerifyRunnerService(self.test_context, self.kafka)
        self.run_rolling_bounce(from_version, to_version, clean_shutdown)

    @cluster(num_nodes=9)
    @matrix(from_version=eos_v1_versions,
            to_version=eos_v2_versions,
            clean_shutdown=[True, False])
    def test_eos_upgrade_complex(self, from_version, to_version, clean_shutdown):
        self.processor1 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor2 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor3 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.verifier = StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka)
        self.run_rolling_bounce(from_version, to_version, clean_shutdown)

    def run_rolling_bounce(self, from_version, to_version, clean_shutdown):
        """
        Rolling bounce from version from_version to version to_version
        """

        self.driver.start()
        self.start_all_nodes_with(from_version)

        self.processors = [self.processor1, self.processor2, self.processor3]

        counter = 1
        random.seed()

        # rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            p.CLEAN_NODE_ENABLED = False
            self.do_stop_start_bounce(p, from_version[:-2], to_version, "exactly_once_v2", counter, clean_shutdown)
            counter = counter + 1

        # shutdown
        self.driver.stop()
        for p in self.processors:
            verify_stopped(p, "StateChange: PENDING_SHUTDOWN -> NOT_RUNNING")

        self.verifier.start()
        self.verifier.wait()
        self.verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.verifier.STDOUT_FILE, allow_fail=False)

    @staticmethod
    def prepare_for(processor, version):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def start_all_nodes_with(self, version):
        kafka_version_str = self.get_version_string(version)

        # start first with <version>
        self.prepare_for(self.processor1, version)
        node1 = self.processor1.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as monitor:
            with node1.account.monitor_log(self.processor1.LOG_FILE) as log_monitor:
                self.processor1.start()
                wait_for(log_monitor, self.processor1, kafka_version_str)
                wait_for(monitor, self.processor1, self.processed_data_msg)

        # start second with <version>
        self.prepare_for(self.processor2, version)
        node2 = self.processor2.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node2.account.monitor_log(self.processor2.LOG_FILE) as log_monitor:
                    self.processor2.start()
                    wait_for(log_monitor, self.processor2, kafka_version_str)
                    wait_for(first_monitor, self.processor1, self.processed_data_msg)
                    wait_for(second_monitor, self.processor2, self.processed_data_msg)

        # start third with <version>
        self.prepare_for(self.processor3, version)
        node3 = self.processor3.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node3.account.monitor_log(self.processor3.STDOUT_FILE) as third_monitor:
                    with node3.account.monitor_log(self.processor3.LOG_FILE) as log_monitor:
                        self.processor3.start()
                        wait_for(log_monitor, self.processor3, kafka_version_str)
                        wait_for(first_monitor, self.processor1, self.processed_data_msg)
                        wait_for(second_monitor, self.processor2, self.processed_data_msg)
                        wait_for(third_monitor, self.processor3, self.processed_data_msg)

    def do_stop_start_bounce(self, processor, upgrade_from, new_version, processing_guarantee, counter, clean_shutdown):
        kafka_version_str = self.get_version_string(new_version)

        first_other_processor = None
        second_other_processor = None
        for p in self.processors:
            if p != processor:
                if first_other_processor is None:
                    first_other_processor = p
                else:
                    second_other_processor = p

        node = processor.node
        first_other_node = first_other_processor.node
        second_other_node = second_other_processor.node

        # stop processor and wait for rebalance of others
        with first_other_node.account.monitor_log(first_other_processor.STDOUT_FILE) as first_other_monitor:
            with second_other_node.account.monitor_log(second_other_processor.STDOUT_FILE) as second_other_monitor:
                processor.stop(clean_shutdown=clean_shutdown)
                wait_for(first_other_monitor, first_other_processor, self.processed_data_msg)
                wait_for(second_other_monitor, second_other_processor, self.processed_data_msg)
        node.account.ssh_capture("grep UPGRADE-TEST-CLIENT-CLOSED %s" % processor.STDOUT_FILE, allow_fail=False)

        if upgrade_from is None:  # upgrade disabled -- second round of rolling bounces
            roll_counter = ".1-"  # second round of rolling bounces
        else:
            roll_counter = ".0-"  # first  round of rolling bounces

        node.account.ssh("mv " + processor.STDOUT_FILE + " " + processor.STDOUT_FILE + roll_counter + str(counter), allow_fail=False)
        node.account.ssh("mv " + processor.STDERR_FILE + " " + processor.STDERR_FILE + roll_counter + str(counter), allow_fail=False)
        node.account.ssh("mv " + processor.LOG_FILE + " " + processor.LOG_FILE + roll_counter + str(counter), allow_fail=False)

        if new_version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(new_version)
        processor.set_processing_guarantee(processing_guarantee)

        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                with first_other_node.account.monitor_log(first_other_processor.STDOUT_FILE) as first_other_monitor:
                    with second_other_node.account.monitor_log(second_other_processor.STDOUT_FILE) as second_other_monitor:
                        processor.start()
                        wait_for(log_monitor, processor, kafka_version_str)
                        wait_for(first_other_monitor, first_other_processor, self.processed_data_msg)
                        wait_for(second_other_monitor, second_other_processor, self.processed_data_msg)
                        wait_for(monitor, processor, self.processed_data_msg)

    def get_version_string(self, version):
        if version.startswith("0") or version.startswith("1") \
            or version.startswith("2.0") or version.startswith("2.1"):
            return "Kafka version : " + version
        elif "SNAPSHOT" in version:
            return "Kafka version.*" + self.base_version_number + ".*SNAPSHOT"
        else:
            return "Kafka version: " + version