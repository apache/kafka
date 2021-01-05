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
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, \
    LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3
from kafkatest.services.streams import CooperativeRebalanceUpgradeService
from kafkatest.tests.streams.utils import verify_stopped, stop_processors, verify_running


class StreamsCooperativeRebalanceUpgradeTest(Test):
    """
    Test of a rolling upgrade from eager rebalance to
    cooperative rebalance
    """

    source_topic = "source"
    sink_topic = "sink"
    task_delimiter = "#"
    report_interval = "1000"
    processing_message = "Processed [0-9]* records so far"
    stopped_message = "COOPERATIVE-REBALANCE-TEST-CLIENT-CLOSED"
    running_state_msg = "STREAMS in a RUNNING State"
    cooperative_turned_off_msg = "Eager rebalancing enabled now for upgrade from %s"
    cooperative_enabled_msg = "Cooperative rebalancing enabled now"
    first_bounce_phase = "first_bounce_phase-"
    second_bounce_phase = "second_bounce_phase-"

    # !!CAUTION!!: THIS LIST OF VERSIONS IS FIXED, NO VERSIONS MUST BE ADDED
    streams_eager_rebalance_upgrade_versions = [str(LATEST_0_10_0), str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0),
                                                str(LATEST_1_0), str(LATEST_1_1), str(LATEST_2_0), str(LATEST_2_1), str(LATEST_2_2),
                                                str(LATEST_2_3)]

    def __init__(self, test_context):
        super(StreamsCooperativeRebalanceUpgradeTest, self).__init__(test_context)
        self.topics = {
            self.source_topic: {'partitions': 9},
            self.sink_topic: {'partitions': 9}
        }

        self.zookeeper = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zookeeper, topics=self.topics)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.source_topic,
                                           throughput=1000,
                                           acks=1)

    @matrix(upgrade_from_version=streams_eager_rebalance_upgrade_versions)
    def test_upgrade_to_cooperative_rebalance(self, upgrade_from_version):
        self.zookeeper.start()
        self.kafka.start()

        processor1 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)
        processor2 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)
        processor3 = CooperativeRebalanceUpgradeService(self.test_context, self.kafka)

        processors = [processor1, processor2, processor3]

        # produce records continually during the test
        self.producer.start()

        # start all processors without upgrade_from config; normal operations mode
        self.logger.info("Starting all streams clients in normal running mode")
        for processor in processors:
            processor.set_version(upgrade_from_version)
            self.set_props(processor)
            processor.CLEAN_NODE_ENABLED = False
            # can't use state as older version don't have state listener
            # so just verify up and running
            verify_running(processor, self.processing_message)

        # all running rebalancing has ceased
        for processor in processors:
            self.verify_processing(processor, self.processing_message)

        # first rolling bounce with "upgrade.from" config set
        previous_phase = ""
        self.maybe_upgrade_rolling_bounce_and_verify(processors,
                                                     previous_phase,
                                                     self.first_bounce_phase,
                                                     upgrade_from_version)

        # All nodes processing, rebalancing has ceased
        for processor in processors:
            self.verify_processing(processor, self.first_bounce_phase + self.processing_message)

        # second rolling bounce without "upgrade.from" config
        self.maybe_upgrade_rolling_bounce_and_verify(processors,
                                                     self.first_bounce_phase,
                                                     self.second_bounce_phase)

        # All nodes processing, rebalancing has ceased
        for processor in processors:
            self.verify_processing(processor, self.second_bounce_phase + self.processing_message)

        # now verify tasks are unique
        for processor in processors:
            self.get_tasks_for_processor(processor)
            self.logger.info("Active tasks %s" % processor.active_tasks)

        overlapping_tasks = processor1.active_tasks.intersection(processor2.active_tasks)
        assert len(overlapping_tasks) == int(0), \
            "Final task assignments are not unique %s %s" % (processor1.active_tasks, processor2.active_tasks)

        overlapping_tasks = processor1.active_tasks.intersection(processor3.active_tasks)
        assert len(overlapping_tasks) == int(0), \
            "Final task assignments are not unique %s %s" % (processor1.active_tasks, processor3.active_tasks)

        overlapping_tasks = processor2.active_tasks.intersection(processor3.active_tasks)
        assert len(overlapping_tasks) == int(0), \
            "Final task assignments are not unique %s %s" % (processor2.active_tasks, processor3.active_tasks)

        # test done close all down
        stop_processors(processors, self.second_bounce_phase + self.stopped_message)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    def maybe_upgrade_rolling_bounce_and_verify(self,
                                                processors,
                                                previous_phase,
                                                current_phase,
                                                upgrade_from_version=None):
        for processor in processors:
            # stop the processor in prep for setting "update.from" or removing "update.from"
            verify_stopped(processor, previous_phase + self.stopped_message)
            # upgrade to version with cooperative rebalance
            processor.set_version("")
            processor.set_upgrade_phase(current_phase)

            if upgrade_from_version is not None:
                # need to remove minor version numbers for check of valid upgrade from numbers
                upgrade_version = upgrade_from_version[:upgrade_from_version.rfind('.')]
                rebalance_mode_msg = self.cooperative_turned_off_msg % upgrade_version
            else:
                upgrade_version = None
                rebalance_mode_msg = self.cooperative_enabled_msg

            self.set_props(processor, upgrade_version)
            node = processor.node
            with node.account.monitor_log(processor.STDOUT_FILE) as stdout_monitor:
                with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                    processor.start()
                    # verify correct rebalance mode either turned off for upgrade or enabled after upgrade
                    log_monitor.wait_until(rebalance_mode_msg,
                                           timeout_sec=60,
                                           err_msg="Never saw '%s' message " % rebalance_mode_msg + str(processor.node.account))

                # verify rebalanced into a running state
                rebalance_msg = current_phase + self.running_state_msg
                stdout_monitor.wait_until(rebalance_msg,
                                          timeout_sec=60,
                                          err_msg="Never saw '%s' message " % rebalance_msg + str(
                                              processor.node.account))

                # verify processing
                verify_processing_msg = current_phase + self.processing_message
                stdout_monitor.wait_until(verify_processing_msg,
                                          timeout_sec=60,
                                          err_msg="Never saw '%s' message " % verify_processing_msg + str(
                                              processor.node.account))

    def verify_processing(self, processor, pattern):
        self.logger.info("Verifying %s processing pattern in STDOUT_FILE" % pattern)
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            monitor.wait_until(pattern,
                               timeout_sec=60,
                               err_msg="Never saw processing of %s " % pattern + str(processor.node.account))

    def get_tasks_for_processor(self, processor):
        retries = 0
        while retries < 5:
            found_tasks = list(processor.node.account.ssh_capture("grep TASK-ASSIGNMENTS %s | tail -n 1" % processor.STDOUT_FILE, allow_fail=True))
            self.logger.info("Returned %s from assigned task check" % found_tasks)
            if len(found_tasks) > 0:
                task_string = str(found_tasks[0]).strip()
                self.logger.info("Converted %s from assigned task check" % task_string)
                processor.set_tasks(task_string)
                return
            retries += 1
            time.sleep(1)
        return

    def set_props(self, processor, upgrade_from=None):
        processor.SOURCE_TOPIC = self.source_topic
        processor.SINK_TOPIC = self.sink_topic
        processor.REPORT_INTERVAL = self.report_interval
        processor.UPGRADE_FROM = upgrade_from
