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
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService, \
    StreamsUpgradeTestJobRunnerService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.streams.utils import extract_generation_from_logs, extract_generation_id
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, \
    LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, LATEST_2_7, LATEST_2_8, \
    LATEST_3_0, LATEST_3_1, LATEST_3_2, DEV_BRANCH, DEV_VERSION, KafkaVersion

# broker 0.10.0 is not compatible with newer Kafka Streams versions
# broker 0.10.1 and 0.10.2 do not support headers, as required by suppress() (since v2.2.1)
broker_upgrade_versions = [str(LATEST_0_11_0), str(LATEST_1_0), str(LATEST_1_1),
                           str(LATEST_2_0), str(LATEST_2_1), str(LATEST_2_2), str(LATEST_2_3),
                           str(LATEST_2_4), str(LATEST_2_5), str(LATEST_2_6), str(LATEST_2_7),
                           str(LATEST_2_8), str(LATEST_3_0), str(LATEST_3_1), str(LATEST_3_2),
                           str(DEV_BRANCH)]

metadata_1_versions = [str(LATEST_0_10_0)]
metadata_2_versions = [str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(LATEST_1_1)]
fk_join_versions = [str(LATEST_2_4), str(LATEST_2_5), str(LATEST_2_6), str(LATEST_2_7), str(LATEST_2_8), 
                    str(LATEST_3_0), str(LATEST_3_1), str(LATEST_3_2)]

"""
After each release one should first check that the released version has been uploaded to 
https://s3-us-west-2.amazonaws.com/kafka-packages/ which is the url used by system test to download jars; 
anyone can verify that by calling 
curl https://s3-us-west-2.amazonaws.com/kafka-packages/kafka_$scala_version-$version.tgz to download the jar
and if it is not uploaded yet, ping the dev@kafka mailing list to request it being uploaded.

This test needs to get updated, but this requires several steps,
which are outlined here:

1. Update all relevant versions in tests/kafkatest/version.py this will include adding a new version for the new
   release and bumping all relevant already released versions.
   
2. Add the new version to the "kafkatest.version" import above and include the version in the 
   broker_upgrade_versions list above.  You'll also need to add the new version to the 
   "StreamsUpgradeTestJobRunnerService" on line 484 to make sure the correct arguments are passed
   during the system test run.
   
3. Update the vagrant/base.sh file to include all new versions, including the newly released version
   and all point releases for existing releases. You only need to list the latest version in 
   this file.
   
4. Then update all relevant versions in the tests/docker/Dockerfile

5. Add a new upgrade-system-tests-XXXX module under streams. You can probably just copy the 
   latest system test module from the last release. Just make sure to update the systout print
   statement in StreamsUpgradeTest to the version for the release. After you add the new module
   you'll need to update settings.gradle file to include the name of the module you just created
   for gradle to recognize the newly added module.

6. Then you'll need to update any version changes in gradle/dependencies.gradle

"""


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
            'fk' : { 'partitions': 5 },
        }

    processed_data_msg = "processed [0-9]* records from topic=data"
    processed_fk_msg = "processed [0-9]* records from topic=fk"
    base_version_number = str(DEV_VERSION).split("-")[0]

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
        self.num_kafka_nodes = 3
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
        self.kafka = KafkaService(self.test_context, num_nodes=self.num_kafka_nodes,
                                  zk=self.zk, version=KafkaVersion(from_version), topics=self.topics)
        self.kafka.start()

        # allow some time for topics to be created
        wait_until(lambda: self.confirm_topics_on_all_brokers(set(self.topics.keys())),
                   timeout_sec=60,
                   err_msg="Broker did not create all topics in 60 seconds ")

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)

        processor = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka, "at_least_once")

        with self.driver.node.account.monitor_log(self.driver.STDOUT_FILE) as driver_monitor:
            self.driver.start()

            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                processor.start()
                monitor.wait_until(self.processed_data_msg,
                                   timeout_sec=60,
                                   err_msg="Never saw output '%s' on " % self.processed_data_msg + str(processor.node))

            connected_message = "Discovered group coordinator"
            with processor.node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                with processor.node.account.monitor_log(processor.STDOUT_FILE) as stdout_monitor:
                    self.perform_broker_upgrade(to_version)

                    log_monitor.wait_until(connected_message,
                                           timeout_sec=120,
                                           err_msg=("Never saw output '%s' on " % connected_message) + str(processor.node.account))

                    stdout_monitor.wait_until(self.processed_data_msg,
                                              timeout_sec=60,
                                              err_msg="Never saw output '%s' on" % self.processed_data_msg + str(processor.node.account))

            # SmokeTestDriver allows up to 6 minutes to consume all
            # records for the verification step so this timeout is set to
            # 6 minutes (360 seconds) for consuming of verification records
            # and a very conservative additional 2 minutes (120 seconds) to process
            # the records in the verification step
            driver_monitor.wait_until('ALL-RECORDS-DELIVERED\|PROCESSED-MORE-THAN-GENERATED',
                                      timeout_sec=480,
                                      err_msg="Never saw output '%s' on" % 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED' + str(self.driver.node.account))

        self.driver.stop()
        processor.stop()
        processor.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % processor.STDOUT_FILE, allow_fail=False)

    @cluster(num_nodes=6)
    @matrix(from_version=metadata_1_versions, to_version=[str(DEV_VERSION)])
    @matrix(from_version=metadata_2_versions, to_version=[str(DEV_VERSION)])
    @matrix(from_version=fk_join_versions, to_version=[str(DEV_VERSION)])
    def test_rolling_upgrade_with_2_bounces(self, from_version, to_version):
        """
        This test verifies that the cluster successfully upgrades despite changes in the metadata and FK
        join protocols.
        
        Starts 3 KafkaStreams instances with version <from_version> and upgrades one-by-one to <to_version>
        """

        if KafkaVersion(from_version).supports_fk_joins() and KafkaVersion(to_version).supports_fk_joins():
            extra_properties = {'test.run_fk_join': 'true'}
        else:
            extra_properties = {}

        self.set_up_services()

        self.driver.start()

        self.start_all_nodes_with(from_version, extra_properties)

        counter = 1
        random.seed()

        # first rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            p.CLEAN_NODE_ENABLED = False
            self.do_stop_start_bounce(p, from_version[:-2], to_version, counter, extra_properties)
            counter = counter + 1

        # second rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            self.do_stop_start_bounce(p, None, to_version, counter, extra_properties)
            counter = counter + 1

        self.stop_and_await()

    @cluster(num_nodes=6)
    def test_version_probing_upgrade(self):
        """
        Starts 3 KafkaStreams instances, and upgrades one-by-one to "future version"
        """

        self.set_up_services()

        self.driver.start()
        self.start_all_nodes_with("") # run with TRUNK

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

        self.stop_and_await()

    @cluster(num_nodes=6)
    @matrix(from_version=[str(LATEST_3_2), str(DEV_VERSION)], to_version=[str(DEV_VERSION)], upgrade=[True, False])
    def test_upgrade_downgrade_state_updater(self, from_version, to_version, upgrade):
        """
        Starts 3 KafkaStreams instances, and enables / disables state restoration
        for the instances in a rolling bounce.

        Once same-thread state restoration is removed from the code, this test
        should use different versions of the code.
        """
        if upgrade:
            extra_properties_first = { '__state.updater.enabled__': 'false' }
            first_version = from_version
            extra_properties_second = { '__state.updater.enabled__': 'true' }
            second_version = to_version
        else:
            extra_properties_first = { '__state.updater.enabled__': 'true' }
            first_version = to_version
            extra_properties_second = { '__state.updater.enabled__': 'false' }
            second_version = from_version

        self.set_up_services()

        self.driver.start()
        self.start_all_nodes_with(first_version, extra_properties_first)

        counter = 1
        random.seed()

        # rolling bounce
        random.shuffle(self.processors)
        for p in self.processors:
            p.CLEAN_NODE_ENABLED = True
            self.do_stop_start_bounce(p, None, second_version, counter, extra_properties_second)
            counter = counter + 1

        self.stop_and_await()

    def set_up_services(self):
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, topics=self.topics)
        self.kafka.start()

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.driver.disable_auto_terminate()
        self.processor1 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        self.processor2 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        self.processor3 = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)

        self.processors = [self.processor1, self.processor2, self.processor3]

    def stop_and_await(self):
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

    def start_all_nodes_with(self, version, extra_properties = None):
        if extra_properties is None:
            extra_properties = {}
        kafka_version_str = self.get_version_string(version)

        # start first with <version>
        self.prepare_for(self.processor1, version, extra_properties)
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
                if extra_properties.get('test.run_fk_join', False):
                    monitor.wait_until(self.processed_fk_msg,
                    timeout_sec=60,
                    err_msg="Never saw output '%s' on " % self.processed_fk_msg + str(node1.account))


        # start second with <version>
        self.prepare_for(self.processor2, version, extra_properties)
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
                    if extra_properties.get('test.run_fk_join', False):
                        second_monitor.wait_until(self.processed_fk_msg,
                        timeout_sec=60,
                        err_msg="Never saw output '%s' on " % self.processed_fk_msg + str(node2.account))

        # start third with <version>
        self.prepare_for(self.processor3, version, extra_properties)
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
                        if extra_properties.get('test.run_fk_join', False):
                            third_monitor.wait_until(self.processed_fk_msg,
                            timeout_sec=60,
                            err_msg="Never saw output '%s' on " % self.processed_fk_msg + str(node2.account))

    @staticmethod
    def prepare_for(processor, version, extra_properties):
        processor.node.account.ssh("rm -rf " + processor.PERSISTENT_ROOT, allow_fail=False)
        for k, v in extra_properties.items():
            processor.set_config(k, v)
        if version == str(DEV_VERSION):
            processor.set_version("")  # set to TRUNK
        else:
            processor.set_version(version)

    def do_stop_start_bounce(self, processor, upgrade_from, new_version, counter, extra_properties = None):
        if extra_properties is None:
            extra_properties = {}

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
        processor.set_upgrade_from(upgrade_from)
        for k, v in extra_properties.items():
            processor.set_config(k, v)

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

                        if extra_properties.get('test.run_fk_join', False):
                            monitor.wait_until(self.processed_fk_msg,
                                               timeout_sec=60,
                                               err_msg="Never saw output '%s' on " % self.processed_fk_msg + str(node.account))


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

        kafka_version_str = self.get_version_string(str(DEV_VERSION))

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
                    log_monitor.wait_until(kafka_version_str,
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

                    highest_version = 11
                    version_probing_message = "Sent a version " + str(highest_version + 1) + " subscription and got version " + str(highest_version) + " assignment back (successful version probing). Downgrade subscription metadata to commonly supported version " + str(highest_version) + " and trigger new rebalance."
                    end_of_upgrade_message = "Sent a version " + str(highest_version) + " subscription and group.s latest commonly supported version is " + str(highest_version + 1) + " (successful version probing and end of rolling upgrade). Upgrading subscription metadata version to " + str(highest_version + 1) + " for next rebalance."
                    end_of_upgrade_error_message = "Could not detect 'successful version probing and end of rolling upgrade' at upgraded node "
                    followup_rebalance_message = "Triggering the followup rebalance scheduled for"
                    followup_rebalance_error_message = "Could not detect '" + followup_rebalance_message + "' at node "
                    if len(self.old_processors) > 0:
                        log_monitor.wait_until(version_probing_message,
                                               timeout_sec=60,
                                               err_msg="Could not detect 'successful version probing' at upgrading node " + str(node.account))
                        log_monitor.wait_until(followup_rebalance_message,
                                               timeout_sec=60,
                                               err_msg=followup_rebalance_error_message + str(node.account))
                    else:
                        first_other_monitor.wait_until(end_of_upgrade_message,
                                                       timeout_sec=60,
                                                       err_msg=end_of_upgrade_error_message + str(first_other_node.account))
                        first_other_monitor.wait_until(followup_rebalance_message,
                                                       timeout_sec=60,
                                                       err_msg=followup_rebalance_error_message + str(node.account))
                        second_other_monitor.wait_until(end_of_upgrade_message,
                                                        timeout_sec=60,
                                                        err_msg=end_of_upgrade_error_message + str(second_other_node.account))
                        second_other_monitor.wait_until(followup_rebalance_message,
                                                        timeout_sec=60,
                                                        err_msg=followup_rebalance_error_message + str(node.account))

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


                    if len(self.old_processors) > 0:
                        self.verify_metadata_no_upgraded_yet(end_of_upgrade_message)

        return current_generation

    def extract_highest_generation(self, found_generations):
        return extract_generation_id(found_generations[-1])

    def verify_metadata_no_upgraded_yet(self, end_of_upgrade_message):
        for p in self.processors:
            found = list(p.node.account.ssh_capture("grep \"" + end_of_upgrade_message + "\" " + p.LOG_FILE, allow_fail=True))
            if len(found) > 0:
                raise Exception("Kafka Streams failed with 'group member upgraded to metadata 8 too early'")

    def confirm_topics_on_all_brokers(self, expected_topic_set):
        for node in self.kafka.nodes:
            match_count = 0
            # need to iterate over topic_list_generator as kafka.list_topics()
            # returns a python generator so values are fetched lazily
            # so we can't just compare directly we must iterate over what's returned
            topic_list_generator = self.kafka.list_topics(node=node)
            for topic in topic_list_generator:
                if topic in expected_topic_set:
                    match_count += 1

            if len(expected_topic_set) != match_count:
                return False

        return True

