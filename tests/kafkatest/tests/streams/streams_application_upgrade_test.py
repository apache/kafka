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
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, DEV_VERSION, KafkaVersion

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

    @cluster(num_nodes=6)
    @matrix(from_version=smoke_test_versions, to_version=dev_version, bounce_type=["full"])
    def test_app_upgrade(self, from_version, to_version, bounce_type):
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

        self.purge_state_dir(self.processor1)
        self.purge_state_dir(self.processor2)
        self.purge_state_dir(self.processor3)

        self.driver.start()
        self.start_all_nodes_with(from_version)

        self.processors = [self.processor1, self.processor2, self.processor3]

        if bounce_type == "rolling":
            counter = 1
            random.seed()
            # upgrade one-by-one via rolling bounce
            random.shuffle(self.processors)
            for p in self.processors:
                p.CLEAN_NODE_ENABLED = False
                self.do_stop_start_bounce(p, None, to_version, counter)
                counter = counter + 1
        elif bounce_type == "full":
            self.restart_all_nodes_with(to_version)
        else:
            raise Exception("Unrecognized bounce_type: " + str(bounce_type))


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

    def start_all_nodes_with(self, version):

        self.set_version(self.processor1, version)
        self.set_version(self.processor2, version)
        self.set_version(self.processor3, version)

        self.processor1.start()
        self.processor2.start()
        self.processor3.start()

        # double-check the version
        kafka_version_str = self.get_version_string(version)
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

    def restart_all_nodes_with(self, version):
        self.processor1.stop_node(self.processor1.node)
        self.processor2.stop_node(self.processor2.node)
        self.processor3.stop_node(self.processor3.node)

        # make sure the members have stopped
        self.wait_for_verification(self.processor1, "SMOKE-TEST-CLIENT-CLOSED", self.processor1.STDOUT_FILE)
        self.wait_for_verification(self.processor2, "SMOKE-TEST-CLIENT-CLOSED", self.processor2.STDOUT_FILE)
        self.wait_for_verification(self.processor3, "SMOKE-TEST-CLIENT-CLOSED", self.processor3.STDOUT_FILE)

        self.roll_logs(self.processor1, ".1-1")
        self.roll_logs(self.processor2, ".1-1")
        self.roll_logs(self.processor3, ".1-1")

        self.set_version(self.processor1, version)
        self.set_version(self.processor2, version)
        self.set_version(self.processor3, version)

        self.processor1.start_node(self.processor1.node)
        self.processor2.start_node(self.processor2.node)
        self.processor3.start_node(self.processor3.node)

        # double-check the version
        kafka_version_str = self.get_version_string(version)
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

    def get_version_string(self, version):
        if version.startswith("0") or version.startswith("1") \
          or version.startswith("2.0") or version.startswith("2.1"):
            return "Kafka version : " + version
        elif "SNAPSHOT" in version:
            return "Kafka version.*" + self.base_version_number + ".*SNAPSHOT"
        else:
            return "Kafka version: " + version

    def wait_for_verification(self, processor, message, file, num_lines=1):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=60,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    def verify_from_file(self, processor, message, file):
        result = processor.node.account.ssh_output("grep -E '%s' %s | wc -l" % (message, file), allow_fail=False)
        try:
            return int(result)
        except ValueError:
            self.logger.warn("Command failed with ValueError: " + result)
            return 0

    def set_version(self, processor, version):
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def purge_state_dir(self, processor):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)

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
                processor.stop_node(processor.node)
                first_other_monitor.wait_until(self.processed_msg,
                                               timeout_sec=60,
                                               err_msg="Never saw output '%s' on " % self.processed_msg + str(first_other_node.account))
                second_other_monitor.wait_until(self.processed_msg,
                                                timeout_sec=60,
                                                err_msg="Never saw output '%s' on " % self.processed_msg + str(second_other_node.account))
        node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % processor.STDOUT_FILE, allow_fail=False)

        if upgrade_from is None:  # upgrade disabled -- second round of rolling bounces
            roll_counter = ".1-"  # second round of rolling bounces
        else:
            roll_counter = ".0-"  # first  round of rolling bounces

        self.roll_logs(processor, roll_counter + str(counter))

        self.set_version(processor, new_version)
        processor.set_upgrade_from(upgrade_from)

        grep_metadata_error = "grep \"org.apache.kafka.streams.errors.TaskAssignmentException: unable to decode subscription data: version=2\" "
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                with first_other_node.account.monitor_log(first_other_processor.STDOUT_FILE) as first_other_monitor:
                    with second_other_node.account.monitor_log(second_other_processor.STDOUT_FILE) as second_other_monitor:
                        processor.start_node(processor.node)

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

    def roll_logs(self, processor, roll_suffix):
        processor.node.account.ssh("mv " + processor.STDOUT_FILE + " " + processor.STDOUT_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.STDERR_FILE + " " + processor.STDERR_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.LOG_FILE + " " + processor.LOG_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.CONFIG_FILE + " " + processor.CONFIG_FILE + roll_suffix,
                                   allow_fail=False)
