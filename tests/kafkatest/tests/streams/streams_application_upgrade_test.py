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
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.version import LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, LATEST_2_7, LATEST_2_8, \
  LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, LATEST_3_7, LATEST_3_8, \
  LATEST_3_9, LATEST_4_0, LATEST_4_1, LATEST_4_2, LATEST_4_3, DEV_VERSION, KafkaVersion


smoke_test_versions = [str(LATEST_2_4), str(LATEST_2_5), str(LATEST_2_6),
                       str(LATEST_2_7), str(LATEST_2_8), str(LATEST_3_0),
                       str(LATEST_3_1), str(LATEST_3_2), str(LATEST_3_3),
                       str(LATEST_3_4), str(LATEST_3_5), str(LATEST_3_6),
                       str(LATEST_3_7), str(LATEST_3_8), str(LATEST_3_9),
                       str(LATEST_4_0), str(LATEST_4_1), str(LATEST_4_2),
                       str(LATEST_4_3)]

# The headers-aware suppress buffer (KAFKA-20413) is only on trunk, so 4.3 is the newest
# release that still writes plain V3 suppress-changelog records even with this config set.
DSL_STORE_FORMAT_CONFIG = "dsl.store.format"
DSL_STORE_FORMAT_HEADERS = "HEADERS"
SUPPRESS_HEADERS_OLD_VERSION = str(LATEST_4_3)

# InMemoryTimeOrderedKeyValueChangeBuffer throws this when it cannot make sense of a
# suppress-changelog record while restoring.
INVALID_CHANGELOG_RECORD_MSG = "Restoring apparently invalid changelog record"

class StreamsUpgradeTest(Test):
    """
    Test upgrading Kafka Streams (all possible version combination)
    Directly upgrading from 2.3 or below is no longer supported as
    of version 4.0
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

    @cluster(num_nodes=9)
    @matrix(from_version=smoke_test_versions, metadata_quorum=[quorum.combined_kraft])
    def test_app_upgrade(self, from_version, metadata_quorum):
        """
        Starts 3 KafkaStreams instances with <old_version>, and upgrades one-by-one to <new_version>
        """
        self._run_app_transition(from_version, str(DEV_VERSION))

    @cluster(num_nodes=9)
    @matrix(direction=["upgrade", "downgrade"], metadata_quorum=[quorum.combined_kraft])
    def test_suppress_headers_app_transition(self, direction, metadata_quorum):
        """
        Same smoke-test application as test_app_upgrade, but with dsl.store.format=HEADERS so that
        suppress() uses the headers-aware buffer (KAFKA-20413).

        The transition crosses the 4.3/trunk boundary in both directions because the two sides write
        the suppress changelog differently even though both tag the record as V3:
          - 4.3 has the dsl.store.format config but not the headers-aware buffer, so it writes the
            whole BufferValue into the record value.
          - trunk writes only the plain value bytes into the record value and ships the
            value/timestamp/headers prefixes in extra Kafka record headers.

        The suppress buffer is in-memory only, so every restart replays its entire changelog. That
        makes this an actual cross-format restore test: on upgrade, trunk must restore records that
        carry no value-part headers; on downgrade, 4.3 must cope with records whose prefixes it never
        learned to read.
        """
        old_version = SUPPRESS_HEADERS_OLD_VERSION
        dev_version = str(DEV_VERSION)

        if direction == "upgrade":
            from_version, to_version = old_version, dev_version
        else:
            from_version, to_version = dev_version, old_version

        self._run_app_transition(
            from_version,
            to_version,
            extra_configs={DSL_STORE_FORMAT_CONFIG: DSL_STORE_FORMAT_HEADERS},
            verify_suppress_restore=True)

    def _run_app_transition(self, from_version, to_version,
                            extra_configs=None, verify_suppress_restore=False):
        """
        Starts 3 KafkaStreams instances with <from_version> and moves them all to <to_version>,
        keeping the smoke-test workload running throughout.
        """

        if from_version == to_version:
            return

        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=None, topics={
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
        }, controller_num_nodes_override=1)
        self.kafka.start()

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.driver.disable_auto_terminate()
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1, extra_configs = extra_configs)
        self.processor2 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1, extra_configs = extra_configs)
        self.processor3 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, processing_guarantee = "at_least_once", replication_factor = 1, extra_configs = extra_configs)

        self.purge_state_dir(self.processor1)
        self.purge_state_dir(self.processor2)
        self.purge_state_dir(self.processor3)

        self.driver.start()
        self.start_all_nodes_with(from_version)

        self.processors = [self.processor1, self.processor2, self.processor3]

        self.restart_all_nodes_with(from_version, to_version)

        if verify_suppress_restore:
            # The liveness checks above only prove the app came back up. A suppress-changelog record
            # that is misread rather than rejected is silent, so also assert that no instance logged
            # a restore rejection while replaying the buffer.
            for p in self.processors:
                self.verify_no_suppress_restore_failure(p)

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

    def restart_all_nodes_with(self, from_version, to_version):
        self.processor1.stop_node(self.processor1.node)
        self.processor2.stop_node(self.processor2.node)
        self.processor3.stop_node(self.processor3.node)

        # make sure the members have stopped
        if from_version.startswith("2."):
            # some older versions crash on shutdown, so we allow crashes here.
            self.wait_for_verification(self.processor1, "SMOKE-TEST-CLIENT-(EXCEPTION|CLOSED)", self.processor1.STDOUT_FILE)
            self.wait_for_verification(self.processor2, "SMOKE-TEST-CLIENT-(EXCEPTION|CLOSED)", self.processor2.STDOUT_FILE)
            self.wait_for_verification(self.processor3, "SMOKE-TEST-CLIENT-(EXCEPTION|CLOSED)", self.processor3.STDOUT_FILE)
        else:
            self.wait_for_verification(self.processor1, "SMOKE-TEST-CLIENT-CLOSED", self.processor1.STDOUT_FILE)
            self.wait_for_verification(self.processor2, "SMOKE-TEST-CLIENT-CLOSED", self.processor2.STDOUT_FILE)
            self.wait_for_verification(self.processor3, "SMOKE-TEST-CLIENT-CLOSED", self.processor3.STDOUT_FILE)

        self.roll_logs(self.processor1, ".1-1")
        self.roll_logs(self.processor2, ".1-1")
        self.roll_logs(self.processor3, ".1-1")

        self.set_version(self.processor1, to_version)
        self.set_version(self.processor2, to_version)
        self.set_version(self.processor3, to_version)

        self.processor1.start_node(self.processor1.node)
        self.processor2.start_node(self.processor2.node)
        self.processor3.start_node(self.processor3.node)

        # double-check the version
        kafka_version_str = self.get_version_string(to_version)
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

    def verify_no_suppress_restore_failure(self, processor):
        node = processor.node
        for file in [processor.STDERR_FILE, processor.LOG_FILE]:
            found = list(node.account.ssh_capture(
                "grep -F '%s' %s" % (INVALID_CHANGELOG_RECORD_MSG, file), allow_fail=True))
            if len(found) > 0:
                raise Exception("Suppress buffer failed to restore its changelog on %s: %s"
                                % (str(node.account), found[0]))

    def set_version(self, processor, version):
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def purge_state_dir(self, processor):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)

    def roll_logs(self, processor, roll_suffix):
        processor.node.account.ssh("mv " + processor.STDOUT_FILE + " " + processor.STDOUT_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.STDERR_FILE + " " + processor.STDERR_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.LOG_FILE + " " + processor.LOG_FILE + roll_suffix,
                                   allow_fail=False)
        processor.node.account.ssh("mv " + processor.CONFIG_FILE + " " + processor.CONFIG_FILE + roll_suffix,
                                   allow_fail=False)
