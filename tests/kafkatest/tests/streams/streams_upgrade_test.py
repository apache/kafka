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
from ducktape.mark import ignore, matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService, StreamsUpgradeTestJobRunnerService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, DEV_BRANCH, DEV_VERSION, KafkaVersion

# broker 0.10.0 is not compatible with newer Kafka Streams versions
broker_upgrade_versions = [str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(LATEST_1_1), str(DEV_BRANCH)]

metadata_1_versions = [str(LATEST_0_10_0)]
metadata_2_versions = [str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(LATEST_1_1)]
# we can add the following versions to `backward_compatible_metadata_2_versions` after the corresponding
# bug-fix release 0.10.1.2, 0.10.2.2, 0.11.0.3, 1.0.2, and 1.1.1 are available:
# str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(LATEST_1_1)
backward_compatible_metadata_2_versions = []
metadata_3_versions = [str(DEV_VERSION)]

class StreamsUpgradeTest(Test):
    """
    Test upgrading Kafka Streams (all version combination)
    If metadata was changes, upgrade is more difficult
    Metadata version was bumped in 0.10.1.0
    """

    def __init__(self, test_context):
        super(StreamsUpgradeTest, self).__init__(test_context)
        self.topics = {
            'echo' : { 'partitions': 5 },
            'data' : { 'partitions': 5 },
        }
        self.leader = None
        self.leader_counter = {}

    def perform_broker_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling broker upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion(to_version)
            self.kafka.start_node(node)

    @ignore
    @cluster(num_nodes=6)
    @matrix(from_version=broker_upgrade_versions, to_version=broker_upgrade_versions)
    def test_upgrade_downgrade_brokers(self, from_version, to_version):
        """
        Start a smoke test client then perform rolling upgrades on the broker.
        """

        if from_version == to_version:
            return

        self.replication = 3
        self.partitions = 1
        self.isr = 2
        self.topics = {
            'echo' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr}},
            'data' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} },
            'min' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'max' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'sum' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'dif' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'cnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'avg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'wcnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} },
            'tagg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} }
        }

        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # number of nodes needs to be >= 3 for the smoke test
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zk, version=KafkaVersion(from_version), topics=self.topics)
        self.kafka.start()

        # allow some time for topics to be created
        time.sleep(10)

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)
        
        self.driver.start()
        self.processor1.start()
        time.sleep(15)

        self.perform_broker_upgrade(to_version)

        time.sleep(15)
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
        self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=False)

    @ignore
    @matrix(from_version=metadata_2_versions, to_version=metadata_2_versions)
    def test_simple_upgrade_downgrade(self, from_version, to_version):
        """
        Starts 3 KafkaStreams instances with <old_version>, and upgrades one-by-one to <new_version>
        """

        if from_version == to_version:
            return

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
        self.driver.wait()

        random.shuffle(self.processors)
        for p in self.processors:
            node = p.node
            with node.account.monitor_log(p.STDOUT_FILE) as monitor:
                p.stop()
                monitor.wait_until("UPGRADE-TEST-CLIENT-CLOSED",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'UPGRADE-TEST-CLIENT-CLOSED' on" + str(node.account))

        self.driver.stop()

    @matrix(from_version=metadata_1_versions, to_version=backward_compatible_metadata_2_versions)
    @matrix(from_version=metadata_1_versions, to_version=metadata_3_versions)
    @matrix(from_version=metadata_2_versions, to_version=metadata_3_versions)
    def test_metadata_upgrade(self, from_version, to_version):
        """
        Starts 3 KafkaStreams instances with version <from_version> and upgrades one-by-one to <to_version>
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
        self.start_all_nodes_with(from_version)

        self.processors = [self.processor1, self.processor2, self.processor3]

        counter = 1
        random.seed()

        # first rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            p.CLEAN_NODE_ENABLED = False
            self.do_stop_start_bounce(p, from_version[:-2], to_version, counter)
            counter = counter + 1

        # second rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            self.do_stop_start_bounce(p, None, to_version, counter)
            counter = counter + 1

        # shutdown
        self.driver.stop()
        self.driver.wait()

        random.shuffle(self.processors)
        for p in self.processors:
            node = p.node
            with node.account.monitor_log(p.STDOUT_FILE) as monitor:
                p.stop()
                monitor.wait_until("UPGRADE-TEST-CLIENT-CLOSED",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'UPGRADE-TEST-CLIENT-CLOSED' on" + str(node.account))

        self.driver.stop()

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
        for p in self.processors:
            self.leader_counter[p] = 2

        self.update_leader()
        for p in self.processors:
            self.leader_counter[p] = 0
        self.leader_counter[self.leader] = 3

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
        self.driver.wait()

        random.shuffle(self.processors)
        for p in self.processors:
            node = p.node
            with node.account.monitor_log(p.STDOUT_FILE) as monitor:
                p.stop()
                monitor.wait_until("UPGRADE-TEST-CLIENT-CLOSED",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'UPGRADE-TEST-CLIENT-CLOSED' on" + str(node.account))

        self.driver.stop()

    def update_leader(self):
        self.leader = None
        retries = 10
        while retries > 0:
            for p in self.processors:
                found = list(p.node.account.ssh_capture("grep \"Finished assignment for group\" %s" % p.LOG_FILE, allow_fail=True))
                if len(found) == self.leader_counter[p] + 1:
                    if self.leader is not None:
                        raise Exception("Could not uniquely identify leader")
                    self.leader = p
                    self.leader_counter[p] = self.leader_counter[p] + 1

            if self.leader is None:
                retries = retries - 1
                time.sleep(5)
            else:
                break

        if self.leader is None:
            raise Exception("Could not identify leader")

    def start_all_nodes_with(self, version):
        # start first with <version>
        self.prepare_for(self.processor1, version)
        node1 = self.processor1.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as monitor:
            with node1.account.monitor_log(self.processor1.LOG_FILE) as log_monitor:
                self.processor1.start()
                log_monitor.wait_until("Kafka version : " + version,
                                       timeout_sec=60,
                                       err_msg="Could not detect Kafka Streams version " + version + " " + str(node1.account))
                monitor.wait_until("processed 100 records from topic",
                                   timeout_sec=60,
                                   err_msg="Never saw output 'processed 100 records from topic' on" + str(node1.account))

        # start second with <version>
        self.prepare_for(self.processor2, version)
        node2 = self.processor2.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node2.account.monitor_log(self.processor2.LOG_FILE) as log_monitor:
                    self.processor2.start()
                    log_monitor.wait_until("Kafka version : " + version,
                                           timeout_sec=60,
                                           err_msg="Could not detect Kafka Streams version " + version + " " + str(node2.account))
                    first_monitor.wait_until("processed 100 records from topic",
                                             timeout_sec=60,
                                             err_msg="Never saw output 'processed 100 records from topic' on" + str(node1.account))
                    second_monitor.wait_until("processed 100 records from topic",
                                              timeout_sec=60,
                                              err_msg="Never saw output 'processed 100 records from topic' on" + str(node2.account))

        # start third with <version>
        self.prepare_for(self.processor3, version)
        node3 = self.processor3.node
        with node1.account.monitor_log(self.processor1.STDOUT_FILE) as first_monitor:
            with node2.account.monitor_log(self.processor2.STDOUT_FILE) as second_monitor:
                with node3.account.monitor_log(self.processor3.STDOUT_FILE) as third_monitor:
                    with node3.account.monitor_log(self.processor3.LOG_FILE) as log_monitor:
                        self.processor3.start()
                        log_monitor.wait_until("Kafka version : " + version,
                                               timeout_sec=60,
                                               err_msg="Could not detect Kafka Streams version " + version + " " + str(node3.account))
                        first_monitor.wait_until("processed 100 records from topic",
                                                 timeout_sec=60,
                                                 err_msg="Never saw output 'processed 100 records from topic' on" + str(node1.account))
                        second_monitor.wait_until("processed 100 records from topic",
                                                  timeout_sec=60,
                                                  err_msg="Never saw output 'processed 100 records from topic' on" + str(node2.account))
                        third_monitor.wait_until("processed 100 records from topic",
                                                  timeout_sec=60,
                                                  err_msg="Never saw output 'processed 100 records from topic' on" + str(node3.account))

    @staticmethod
    def prepare_for(processor, version):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def do_stop_start_bounce(self, processor, upgrade_from, new_version, counter):
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
                first_other_monitor.wait_until("processed 100 records from topic",
                                               timeout_sec=60,
                                               err_msg="Never saw output 'processed 100 records from topic' on" + str(first_other_node.account))
                second_other_monitor.wait_until("processed 100 records from topic",
                                                timeout_sec=60,
                                                err_msg="Never saw output 'processed 100 records from topic' on" + str(second_other_node.account))
        node.account.ssh_capture("grep UPGRADE-TEST-CLIENT-CLOSED %s" % processor.STDOUT_FILE, allow_fail=False)

        if upgrade_from is None:  # upgrade disabled -- second round of rolling bounces
            roll_counter = ".1-"  # second round of rolling bounces
        else:
            roll_counter = ".0-"  # first  round of rolling boundes

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

                        log_monitor.wait_until("Kafka version : " + new_version,
                                               timeout_sec=60,
                                               err_msg="Could not detect Kafka Streams version " + new_version + " " + str(node.account))
                        first_other_monitor.wait_until("processed 100 records from topic",
                                                       timeout_sec=60,
                                                       err_msg="Never saw output 'processed 100 records from topic' on" + str(first_other_node.account))
                        found = list(first_other_node.account.ssh_capture(grep_metadata_error + first_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        second_other_monitor.wait_until("processed 100 records from topic",
                                                        timeout_sec=60,
                                                        err_msg="Never saw output 'processed 100 records from topic' on" + str(second_other_node.account))
                        found = list(second_other_node.account.ssh_capture(grep_metadata_error + second_other_processor.STDERR_FILE, allow_fail=True))
                        if len(found) > 0:
                            raise Exception("Kafka Streams failed with 'unable to decode subscription data: version=2'")

                        monitor.wait_until("processed 100 records from topic",
                                           timeout_sec=60,
                                           err_msg="Never saw output 'processed 100 records from topic' on" + str(node.account))

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
                self.leader_counter[processor] = 0

                with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                    processor.set_upgrade_to("future_version")
                    processor.start()
                    self.old_processors.remove(processor)
                    self.upgraded_processors.append(processor)

                    current_generation = current_generation + 1

                    log_monitor.wait_until("Kafka version : " + str(DEV_VERSION),
                                           timeout_sec=60,
                                           err_msg="Could not detect Kafka Streams version " + str(DEV_VERSION) + " in " + str(node.account))
                    log_monitor.offset = 5
                    log_monitor.wait_until("partition\.assignment\.strategy = \[org\.apache\.kafka\.streams\.tests\.StreamsUpgradeTest$FutureStreamsPartitionAssignor\]",
                                           timeout_sec=60,
                                           err_msg="Could not detect FutureStreamsPartitionAssignor in " + str(node.account))

                    log_monitor.wait_until("Successfully joined group with generation " + str(current_generation),
                                           timeout_sec=60,
                                           err_msg="Never saw output 'Successfully joined group with generation " + str(current_generation) + "' on" + str(node.account))
                    first_other_monitor.wait_until("Successfully joined group with generation " + str(current_generation),
                                                   timeout_sec=60,
                                                   err_msg="Never saw output 'Successfully joined group with generation " + str(current_generation) + "' on" + str(first_other_node.account))
                    second_other_monitor.wait_until("Successfully joined group with generation " + str(current_generation),
                                                    timeout_sec=60,
                                                    err_msg="Never saw output 'Successfully joined group with generation " + str(current_generation) + "' on" + str(second_other_node.account))

                    if processor == self.leader:
                        self.update_leader()
                    else:
                        self.leader_counter[self.leader] = self.leader_counter[self.leader] + 1

                    if processor == self.leader:
                        leader_monitor = log_monitor
                    elif first_other_processor == self.leader:
                        leader_monitor = first_other_monitor
                    elif second_other_processor == self.leader:
                        leader_monitor = second_other_monitor
                    else:
                        raise Exception("Could not identify leader.")

                    monitors = {}
                    monitors[processor] = log_monitor
                    monitors[first_other_processor] = first_other_monitor
                    monitors[second_other_processor] = second_other_monitor

                    leader_monitor.wait_until("Received a future (version probing) subscription (version: 4). Sending empty assignment back (with supported version 3).",
                                              timeout_sec=60,
                                              err_msg="Could not detect 'version probing' attempt at leader " + str(self.leader.node.account))

                    if len(self.old_processors) > 0:
                        log_monitor.wait_until("Sent a version 4 subscription and got version 3 assignment back (successful version probing). Downgrading subscription metadata to received version and trigger new rebalance.",
                                               timeout_sec=60,
                                               err_msg="Could not detect 'successful version probing' at upgrading node " + str(node.account))
                    else:
                        log_monitor.wait_until("Sent a version 4 subscription and got version 3 assignment back (successful version probing). Setting subscription metadata to leaders supported version 4 and trigger new rebalance.",
                                               timeout_sec=60,
                                               err_msg="Could not detect 'successful version probing with upgraded leader' at upgrading node " + str(node.account))
                        first_other_monitor.wait_until("Sent a version 3 subscription and group leader.s latest supported version is 4. Upgrading subscription metadata version to 4 for next rebalance.",
                                                       timeout_sec=60,
                                                       err_msg="Never saw output 'Upgrade metadata to version 4' on" + str(first_other_node.account))
                        second_other_monitor.wait_until("Sent a version 3 subscription and group leader.s latest supported version is 4. Upgrading subscription metadata version to 4 for next rebalance.",
                                                        timeout_sec=60,
                                                        err_msg="Never saw output 'Upgrade metadata to version 4' on" + str(second_other_node.account))

                    log_monitor.wait_until("Version probing detected. Triggering new rebalance.",
                                           timeout_sec=60,
                                           err_msg="Could not detect 'Triggering new rebalance' at upgrading node " + str(node.account))

                    # version probing should trigger second rebalance
                    current_generation = current_generation + 1

                    for p in self.processors:
                        monitors[p].wait_until("Successfully joined group with generation " + str(current_generation),
                                               timeout_sec=60,
                                               err_msg="Never saw output 'Successfully joined group with generation " + str(current_generation) + "' on" + str(p.node.account))

                    if processor == self.leader:
                        self.update_leader()
                    else:
                        self.leader_counter[self.leader] = self.leader_counter[self.leader] + 1

                    if self.leader in self.old_processors or len(self.old_processors) > 0:
                        self.verify_metadata_no_upgraded_yet()

        return current_generation

    def verify_metadata_no_upgraded_yet(self):
        for p in self.processors:
            found = list(p.node.account.ssh_capture("grep \"Sent a version 3 subscription and group leader.s latest supported version is 4. Upgrading subscription metadata version to 4 for next rebalance.\" " + p.LOG_FILE, allow_fail=True))
            if len(found) > 0:
                raise Exception("Kafka Streams failed with 'group member upgraded to metadata 4 too early'")
