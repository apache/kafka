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
import time
from ducktape.mark import matrix, ignore
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService, \
    StreamsUpgradeTestJobRunnerService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.streams.utils import extract_generation_from_logs
from kafkatest.version import LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, DEV_BRANCH, DEV_VERSION, KafkaVersion

smoke_test_versions = [str(LATEST_2_2), str(LATEST_2_3), str(LATEST_2_4), str(LATEST_2_5)]
dev_version = [str(DEV_VERSION)]

class StreamsUpgradeTest(Test):
    """
    Test upgrading Kafka Streams (all version combination)
    If metadata was changes, upgrade is more difficult
    Metadata version was bumped in 0.10.1.0 and
    subsequently bumped in 2.0.0
    """

    def __init__(self, test_context):
        super(StreamsUpgradeTest, self).__init__(test_context)
        self.topics = {
            'echo' : { 'partitions': 5 },
            'data' : { 'partitions': 5 },
        }

    processed_msg = "processed [0-9]* records"
    base_version_number = str(DEV_VERSION).split("-")[0]

    def perform_broker_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling broker upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion(to_version)
            self.kafka.start_node(node)

    @matrix(from_version=smoke_test_versions, to_version=dev_version)
    def test_app_upgrade(self, from_version, to_version):
        """
        Starts 3 KafkaStreams instances with <old_version>, and upgrades one-by-one to <new_version>
        """

        if from_version == to_version:
            return

        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, topics={
            'echo' : { 'partitions': 5, 'replication-factor': 1 },
            'data' : { 'partitions': 5, 'replication-factor': 1 },
            'min' : { 'partitions': 5, 'replication-factor': 1 },
            'min-suppressed' : { 'partitions': 5, 'replication-factor': 1 },
            'min-raw' : { 'partitions': 5, 'replication-factor': 1 },
            'max' : { 'partitions': 5, 'replication-factor': 1 },
            'sum' : { 'partitions': 5, 'replication-factor': 1 },
            'sws-raw' : { 'partitions': 5, 'replication-factor': 1 },
            'sws-suppressed' : { 'partitions': 5, 'replication-factor': 1 },
            'dif' : { 'partitions': 5, 'replication-factor': 1 },
            'cnt' : { 'partitions': 5, 'replication-factor': 1 },
            'avg' : { 'partitions': 5, 'replication-factor': 1 },
            'wcnt' : { 'partitions': 5, 'replication-factor': 1 },
            'tagg' : { 'partitions': 5, 'replication-factor': 1 }
        })
        self.kafka.start()

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.driver.disable_auto_terminate()
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1)
        self.processor2 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1)
        self.processor3 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1)

        self.driver.start()
        self.start_all_nodes_with(from_version)

        self.processors = [self.processor1, self.processor2, self.processor3]

        counter = 1
        random.seed()

        # upgrade one-by-one via rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            p.CLEAN_NODE_ENABLED = False
            self.do_stop_start_bounce(p, None, to_version, counter)
            counter = counter + 1

        # shutdown
        self.driver.stop()

        # Ideally, we would actually verify the expected results.
        # See KAFKA-10202

        random.shuffle(self.processors)
        for p in self.processors:
            node = p.node
            with node.account.monitor_log(p.STDOUT_FILE) as monitor:
                p.stop()
                monitor.wait_until("SMOKE-TEST-CLIENT-CLOSED",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'SMOKE-TEST-CLIENT-CLOSED' on " + str(node.account))

    def test_version_probing_upgrade(self):
        """
        Starts 3 KafkaStreams instances, and upgrades one-by-one to "future version"
        """

        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, topics=self.topics)
        self.kafka.start()

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.driver.disable_auto_terminate()
        self.processor1 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        self.processor2 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        self.processor3 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)

        self.driver.start()
        self.start_all_nodes_with("") # run with TRUNK

        self.processors = [self.processor1, self.processor2, self.processor3]
        self.old_processors = [self.processor1, self.processor2, self.processor3]
        self.upgraded_processors = []

        counter = 1
        current_generation = 3

        random.seed()
        random.shuffle(self.processors)

        for p in self.processors:
            p.CLEAN_NODE_ENABLED = False
            current_generation = self.do_rolling_bounce(p, counter, current_generation)
            counter = counter + 1

        # shutdown
        self.driver.stop()

        random.shuffle(self.processors)
        for p in self.processors:
            node = p.node
            with node.account.monitor_log(p.STDOUT_FILE) as monitor:
                p.stop()
                monitor.wait_until("UPGRADE-TEST-CLIENT-CLOSED",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'UPGRADE-TEST-CLIENT-CLOSED' on" + str(node.account))

    def get_version_string(self, version):
        if version.startswith("0") or version.startswith("1") \
          or version.startswith("2.0") or version.startswith("2.1"):
            return "Kafka version : " + version
        elif "SNAPSHOT" in version:
            return "Kafka version.*" + self.base_version_number + ".*SNAPSHOT"
        else:
            return "Kafka version: " + version

    def start_all_nodes_with(self, version):
        kafka_version_str = self.get_version_string(version)

        self.prepare_for(self.processor1, version)
        self.prepare_for(self.processor2, version)
        self.prepare_for(self.processor3, version)

        self.processor1.start()
        self.processor2.start()
        self.processor3.start()

        # double-check the version
        self.wait_for_verification(self.processor1, kafka_version_str, self.processor1.LOG_FILE)
        self.wait_for_verification(self.processor2, kafka_version_str, self.processor2.LOG_FILE)
        self.wait_for_verification(self.processor3, kafka_version_str, self.processor3.LOG_FILE)

        # wait for the members to join
        self.wait_for_verification(self.processor1, "SMOKE-TEST-CLIENT-STARTED", self.processor1.STDOUT_FILE)
        self.wait_for_verification(self.processor2, "SMOKE-TEST-CLIENT-STARTED", self.processor2.STDOUT_FILE)
        self.wait_for_verification(self.processor3, "SMOKE-TEST-CLIENT-STARTED", self.processor3.STDOUT_FILE)

        # make sure they've processed something
        self.wait_for_verification(self.processor1, self.processed_msg, self.processor1.STDOUT_FILE)
        self.wait_for_verification(self.processor2, self.processed_msg, self.processor2.STDOUT_FILE)
        self.wait_for_verification(self.processor3, self.processed_msg, self.processor3.STDOUT_FILE)

    def wait_for_verification(self, processor, message, file, num_lines=1):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=60,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    @staticmethod
    def verify_from_file(processor, message, file):
        result = processor.node.account.ssh_output("grep -E '%s' %s | wc -l" % (message, file), allow_fail=False)
        try:
            return int(result)
        except ValueError:
            self.logger.warn("Command failed with ValueError: " + result)
            return 0

    @staticmethod
    def prepare_for(processor, version):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def do_stop_start_bounce(self, processor, upgrade_from, new_version, counter):
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
                first_other_monitor.wait_until(self.processed_msg,
                                               timeout_sec=60,
                                               err_msg="Never saw output '%s' on " % self.processed_msg + str(first_other_node.account))
                second_other_monitor.wait_until(self.processed_msg,
                                                timeout_sec=60,
                                                err_msg="Never saw output '%s' on " % self.processed_msg + str(second_other_node.account))
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
        processor.set_upgrade_from(upgrade_from)

        grep_metadata_error = "grep \"org.apache.kafka.streams.errors.TaskAssignmentException: unable to decode subscription data: version=2\" "
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                with first_other_node.account.monitor_log(first_other_processor.STDOUT_FILE) as first_other_monitor:
                    with second_other_node.account.monitor_log(second_other_processor.STDOUT_FILE) as second_other_monitor:
                        processor.start()

                        log_monitor.wait_until(kafka_version_str,
                                               timeout_sec=60,
                                               err_msg="Could not detect Kafka Streams version " + new_version + " on " + str(node.account))
                        first_other_monitor.wait_until(self.processed_msg,
                                                       timeout_sec=60,
                                                       err_msg="Never saw output '%s' on " % self.processed_msg + str(first_other_node.account))
                        found = list(first_other_node.account.ssh_capture(grep_metadata_error + first_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        second_other_monitor.wait_until(self.processed_msg,
                                                        timeout_sec=60,
                                                        err_msg="Never saw output '%s' on " % self.processed_msg + str(second_other_node.account))
                        found = list(second_other_node.account.ssh_capture(grep_metadata_error + second_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        monitor.wait_until(self.processed_msg,
                                           timeout_sec=60,
                                           err_msg="Never saw output '%s' on " % self.processed_msg + str(node.account))


    def do_rolling_bounce(self, processor, counter, current_generation):
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

        with first_other_node.account.monitor_log(first_other_processor.LOG_FILE) as first_other_monitor:
            with second_other_node.account.monitor_log(second_other_processor.LOG_FILE) as second_other_monitor:
                # stop processor
                processor.stop()
                node.account.ssh_capture("grep UPGRADE-TEST-CLIENT-CLOSED %s" % processor.STDOUT_FILE, allow_fail=False)

                node.account.ssh("mv " + processor.STDOUT_FILE + " " + processor.STDOUT_FILE + "." + str(counter), allow_fail=False)
                node.account.ssh("mv " + processor.STDERR_FILE + " " + processor.STDERR_FILE + "." + str(counter), allow_fail=False)
                node.account.ssh("mv " + processor.LOG_FILE + " " + processor.LOG_FILE + "." + str(counter), allow_fail=False)

                with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                    processor.set_upgrade_to("future_version")
                    processor.start()
                    self.old_processors.remove(processor)
                    self.upgraded_processors.append(processor)

                    # checking for the dev version which should be the only SNAPSHOT
                    log_monitor.wait_until("Kafka version.*" + self.base_version_number + ".*SNAPSHOT",
                                           timeout_sec=60,
                                           err_msg="Could not detect Kafka Streams version " + str(DEV_VERSION) + " in " + str(node.account))
                    log_monitor.offset = 5
                    log_monitor.wait_until("partition\.assignment\.strategy = \[org\.apache\.kafka\.streams\.tests\.StreamsUpgradeTest$FutureStreamsPartitionAssignor\]",
                                           timeout_sec=60,
                                           err_msg="Could not detect FutureStreamsPartitionAssignor in " + str(node.account))

                    monitors = {}
                    monitors[processor] = log_monitor
                    monitors[first_other_processor] = first_other_monitor
                    monitors[second_other_processor] = second_other_monitor

                    if len(self.old_processors) > 0:
                        log_monitor.wait_until("Sent a version 8 subscription and got version 7 assignment back (successful version probing). Downgrade subscription metadata to commonly supported version 7 and trigger new rebalance.",
                                               timeout_sec=60,
                                               err_msg="Could not detect 'successful version probing' at upgrading node " + str(node.account))
                        log_monitor.wait_until("Triggering the followup rebalance scheduled for 0 ms.",
                                               timeout_sec=60,
                                               err_msg="Could not detect 'Triggering followup rebalance' at upgrading node " + str(node.account))
                    else:
                        first_other_monitor.wait_until("Sent a version 7 subscription and group.s latest commonly supported version is 8 (successful version probing and end of rolling upgrade). Upgrading subscription metadata version to 8 for next rebalance.",
                                                       timeout_sec=60,
                                                       err_msg="Never saw output 'Upgrade metadata to version 8' on" + str(first_other_node.account))
                        first_other_monitor.wait_until("Triggering the followup rebalance scheduled for 0 ms.",
                                                       timeout_sec=60,
                                                       err_msg="Could not detect 'Triggering followup rebalance' at upgrading node " + str(node.account))
                        second_other_monitor.wait_until("Sent a version 7 subscription and group.s latest commonly supported version is 8 (successful version probing and end of rolling upgrade). Upgrading subscription metadata version to 8 for next rebalance.",
                                                        timeout_sec=60,
                                                        err_msg="Never saw output 'Upgrade metadata to version 8' on" + str(second_other_node.account))
                        second_other_monitor.wait_until("Triggering the followup rebalance scheduled for 0 ms.",
                                                        timeout_sec=60,
                                                        err_msg="Could not detect 'Triggering followup rebalance' at upgrading node " + str(node.account))

                    # version probing should trigger second rebalance
                    # now we check that after consecutive rebalances we have synchronized generation
                    generation_synchronized = False
                    retries = 0

                    while retries < 10:
                        processor_found = extract_generation_from_logs(processor)
                        first_other_processor_found = extract_generation_from_logs(first_other_processor)
                        second_other_processor_found = extract_generation_from_logs(second_other_processor)

                        if len(processor_found) > 0 and len(first_other_processor_found) > 0 and len(second_other_processor_found) > 0:
                            self.logger.info("processor: " + str(processor_found))
                            self.logger.info("first other processor: " + str(first_other_processor_found))
                            self.logger.info("second other processor: " + str(second_other_processor_found))

                            processor_generation = self.extract_highest_generation(processor_found)
                            first_other_processor_generation = self.extract_highest_generation(first_other_processor_found)
                            second_other_processor_generation = self.extract_highest_generation(second_other_processor_found)

                            if processor_generation == first_other_processor_generation and processor_generation == second_other_processor_generation:
                                current_generation = processor_generation
                                generation_synchronized = True
                                break

                        time.sleep(5)
                        retries = retries + 1

                    if generation_synchronized == False:
                        raise Exception("Never saw all three processors have the synchronized generation number")


        return current_generation

    def extract_highest_generation(self, found_generations):
        return int(found_generations[-1])
