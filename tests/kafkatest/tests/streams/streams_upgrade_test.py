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

from ducktape.mark import ignore, matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService, StreamsUpgradeTestJobRunnerService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, DEV_BRANCH, DEV_VERSION, KafkaVersion
import random
import time

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

    def perform_broker_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling broker upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion(to_version)
            self.kafka.start_node(node)

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
            self.do_rolling_bounce(p, None, to_version, counter)
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

    #@matrix(from_version=metadata_1_versions, to_version=backward_compatible_metadata_2_versions)
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
            self.do_rolling_bounce(p, from_version[:-2], to_version, counter)
            counter = counter + 1

        # second rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            self.do_rolling_bounce(p, None, to_version, counter)
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

    def do_rolling_bounce(self, processor, upgrade_from, new_version, counter):
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
