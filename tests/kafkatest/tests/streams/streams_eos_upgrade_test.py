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

# TODO: Write upgrade noticex
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
            to_version=eos_v2_versions)
    def test_eos_upgrade_simple(self, from_version, to_version):
        self.processor1 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor2 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor3 = StreamsEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.verifier = StreamsEosTestVerifyRunnerService(self.test_context, self.kafka)
        self.run_rolling_bounce(from_version, to_version)

    @cluster(num_nodes=9)
    @matrix(from_version=eos_v1_versions,
            to_version=eos_v2_versions)
    def test_eos_upgrade_complex(self, from_version, to_version):
        self.processor1 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor2 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.processor3 = StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka, "exactly_once")
        self.verifier = StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka)
        self.run_rolling_bounce(from_version, to_version)

    def run_rolling_bounce(self, from_version, to_version):
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
            self.do_stop_start_bounce(p, from_version[:-2], to_version, "exactly_once_v2", counter)
            counter = counter + 1

        # shutdown
        self.driver.stop()
        for p in self.processors:
            self.stop_streams(p)

        self.verifier.start()
        self.verifier.wait()
        self.verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.verifier.STDOUT_FILE, allow_fail=False)

    def stop_streams(self, processor_to_be_stopped):
        with processor_to_be_stopped.node.account.monitor_log(processor_to_be_stopped.STDOUT_FILE) as monitor2:
            processor_to_be_stopped.stop()
            self.wait_for(monitor2, processor_to_be_stopped, "StateChange: PENDING_SHUTDOWN -> NOT_RUNNING")

    def wait_for_startup(self, monitor, processor):
        self.wait_for(monitor, processor, "StateChange: REBALANCING -> RUNNING")
        self.wait_for(monitor, processor, "processed [0-9]* records from topic")

    def wait_for(self, monitor, processor, output):
        monitor.wait_until(output,
                           timeout_sec=480,
                           err_msg=("Never saw output '%s' on " % output) + str(processor.node.account))

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
                log_monitor.wait_until(kafka_version_str,
                                       timeout_sec=60,
                                       err_msg="Could not detect Kafka Streams version " + version + " " + str(node1.account))
                monitor.wait_until(self.processed_data_msg,
                                   timeout_sec=60,
                                   err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node1.account))


        # start second with <version>
        self.prepare_for(self.processor2, version)
        node2 = self.processor2.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node2.account.monitor_log(self.processor2.LOG_FILE) as log_monitor:
                    self.processor2.start()
                    log_monitor.wait_until(kafka_version_str,
                                           timeout_sec=60,
                                           err_msg="Could not detect Kafka Streams version " + version + " on " + str(node2.account))
                    first_monitor.wait_until(self.processed_data_msg,
                                             timeout_sec=60,
                                             err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node1.account))
                    second_monitor.wait_until(self.processed_data_msg,
                                              timeout_sec=60,
                                              err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node2.account))

        # start third with <version>
        self.prepare_for(self.processor3, version)
        node3 = self.processor3.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node3.account.monitor_log(self.processor3.STDOUT_FILE) as third_monitor:
                    with node3.account.monitor_log(self.processor3.LOG_FILE) as log_monitor:
                        self.processor3.start()
                        log_monitor.wait_until(kafka_version_str,
                                               timeout_sec=60,
                                               err_msg="Could not detect Kafka Streams version " + version + " on " + str(node3.account))
                        first_monitor.wait_until(self.processed_data_msg,
                                                 timeout_sec=60,
                                                 err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node1.account))
                        second_monitor.wait_until(self.processed_data_msg,
                                                  timeout_sec=60,
                                                  err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node2.account))
                        third_monitor.wait_until(self.processed_data_msg,
                                                 timeout_sec=60,
                                                 err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node3.account))

    def do_stop_start_bounce(self, processor, upgrade_from, new_version, processing_guarantee, counter):
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
                processor.stop()
                first_other_monitor.wait_until(self.processed_data_msg,
                                               timeout_sec=60,
                                               err_msg="Never saw output '%s' on " % self.processed_data_msg + str(first_other_node.account))
                second_other_monitor.wait_until(self.processed_data_msg,
                                                timeout_sec=60,
                                                err_msg="Never saw output '%s' on " % self.processed_data_msg + str(second_other_node.account))
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

        grep_metadata_error = "grep \"org.apache.kafka.streams.errors.TaskAssignmentException: unable to decode subscription data: version=2\" "
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                with first_other_node.account.monitor_log(first_other_processor.STDOUT_FILE) as first_other_monitor:
                    with second_other_node.account.monitor_log(second_other_processor.STDOUT_FILE) as second_other_monitor:
                        processor.start()

                        log_monitor.wait_until(kafka_version_str,
                                               timeout_sec=60,
                                               err_msg="Could not detect Kafka Streams version " + new_version + " on " + str(node.account))
                        first_other_monitor.wait_until(self.processed_data_msg,
                                                       timeout_sec=60,
                                                       err_msg="Never saw output '%s' on " % self.processed_data_msg + str(first_other_node.account))
                        found = list(first_other_node.account.ssh_capture(grep_metadata_error + first_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        second_other_monitor.wait_until(self.processed_data_msg,
                                                        timeout_sec=60,
                                                        err_msg="Never saw output '%s' on " % self.processed_data_msg + str(second_other_node.account))
                        found = list(second_other_node.account.ssh_capture(grep_metadata_error + second_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        monitor.wait_until(self.processed_data_msg,
                                           timeout_sec=60,
                                           err_msg="Never saw output '%s' on " % self.processed_data_msg + str(node.account))


    def get_version_string(self, version):
        if version.startswith("0") or version.startswith("1") \
            or version.startswith("2.0") or version.startswith("2.1"):
            return "Kafka version : " + version
        elif "SNAPSHOT" in version:
            return "Kafka version.*" + self.base_version_number + ".*SNAPSHOT"
        else:
            return "Kafka version: " + version