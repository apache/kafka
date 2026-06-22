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

import re

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StaticMemberTestService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.streams.utils import verify_stopped, stop_processors, verify_running, extract_generation_from_logs, extract_generation_id

class StreamsStaticMembershipTest(Test):
    """
    Tests using static membership when broker points to minimum supported
    version (2.3) or higher.
    """

    input_topic = 'inputTopic'
    pattern = 'PROCESSED'
    running_message = 'REBALANCING -> RUNNING'
    stopped_message = 'Static membership test closed'
    num_threads = 3
    num_bounces = 3
    streams_group_protocol = "streams"

    initial_process_id_pattern = re.compile(r"No process id found on disk, got fresh process id ([0-9a-fA-F-]+)")
    random_process_id_pattern = re.compile(r"Created new process id: ([0-9a-fA-F-]+)")
    reused_process_id_pattern = re.compile(r"Reading UUID from process file: ([0-9a-fA-F-]+)")

    def __init__(self, test_context):
        super(StreamsStaticMembershipTest, self).__init__(test_context)
        self.topics = {
            self.input_topic: {'partitions': 18},
        }

        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=None, topics=self.topics, controller_num_nodes_override=1)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.input_topic,
                                           throughput=1000,
                                           acks=1)

    @cluster(num_nodes=8)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_rolling_bounces_will_not_trigger_rebalance_under_static_membership(self, metadata_quorum):
        self.kafka.start()

        processors = self.create_processors(self.num_threads)

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, self.running_message)

        self.verify_processing(processors)

        # do several rolling bounces
        for i in range(0, self.num_bounces):
            for processor in processors:
                verify_stopped(processor, self.stopped_message)
                verify_running(processor, self.running_message)

        stable_generation = -1
        for processor in processors:
            generations = extract_generation_from_logs(processor)
            num_bounce_generations = self.num_bounces * self.num_threads
            assert num_bounce_generations <= len(generations), \
                "Smaller than minimum expected %d generation messages, actual %d" % (num_bounce_generations, len(generations))

            for generation in generations[-num_bounce_generations:]:
                generation = extract_generation_id(generation)
                if stable_generation == -1:
                    stable_generation = generation
                assert stable_generation == generation, \
                    "Stream rolling bounce have caused unexpected generation bump %d" % generation

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop(timeout_sec=120)

    @cluster(num_nodes=8)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_temporary_static_rejoin_does_not_trigger_survivor_reconciliation(self, metadata_quorum):
        self.kafka.start()

        processors = self.create_processors(
            self.num_threads,
            group_protocol=self.streams_group_protocol,
            persistent_process_id_store_enabled=True
        )

        self.producer.start()

        initial_log_checkpoints = {}
        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            initial_log_checkpoints[processor] = self._line_count(processor, processor.LOG_FILE)
            verify_running(processor, self.running_message)

        self.verify_processing(processors)

        baseline_process_ids = {
            processor: self.assert_initial_process_id_persisted(processor, initial_log_checkpoints[processor])
            for processor in processors
        }

        for _ in range(self.num_bounces):
            for bounced in processors:
                checkpoints = {
                    processor: self._line_count(processor, processor.LOG_FILE)
                    for processor in processors
                }

                verify_stopped(bounced, self.stopped_message)
                verify_running(bounced, self.running_message)

                self.assert_same_process_id_reused(
                    bounced,
                    checkpoints[bounced],
                    baseline_process_ids[bounced]
                )

                for survivor in processors:
                    if survivor is not bounced:
                        self.assert_survivor_was_unaffected(survivor, checkpoints[survivor])

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop(timeout_sec=120)

    def create_processors(self, num_threads, group_protocol="classic", persistent_process_id_store_enabled=False):
        return [
            StaticMemberTestService(self.test_context, self.kafka, "consumer-A", num_threads, group_protocol,
                                    persistent_process_id_store_enabled),
            StaticMemberTestService(self.test_context, self.kafka, "consumer-B", num_threads, group_protocol,
                                    persistent_process_id_store_enabled),
            StaticMemberTestService(self.test_context, self.kafka, "consumer-C", num_threads, group_protocol,
                                    persistent_process_id_store_enabled)
        ]

    def verify_processing(self, processors):
        for processor in processors:
            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                monitor.wait_until(self.pattern,
                                   timeout_sec=60,
                                   err_msg="Never saw processing of %s " % self.pattern + str(processor.node.account))

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic

    def thread_instance_ids(self, processor):
        return ["%s-%d" % (processor.GROUP_INSTANCE_ID, thread_id)
                for thread_id in range(1, self.num_threads + 1)]

    def _line_count(self, processor, path):
        output = list(
            processor.node.account.ssh_capture("awk 'END {print NR}' %s" % path, allow_fail=True)
        )
        if not output:
            return 0
        return int(output[0].strip() or 0)

    def _read_lines_since(self, processor, path, line_number):
        first_line = max(1, line_number + 1)
        return list(
            processor.node.account.ssh_capture("sed -n '%d,$p' %s" % (first_line, path), allow_fail=True)
        )

    def assert_initial_process_id_persisted(self, processor, log_checkpoint):
        log = "".join(self._read_lines_since(processor, processor.LOG_FILE, log_checkpoint))

        fresh_matches = self.initial_process_id_pattern.findall(log)
        random_matches = self.random_process_id_pattern.findall(log)

        assert fresh_matches, (
            "Did not see initial persisted process id creation for %s"
            % processor.GROUP_INSTANCE_ID
        )
        assert not random_matches, (
            "Unexpected non-persistent process id creation for %s: %s"
            % (processor.GROUP_INSTANCE_ID, random_matches)
        )

        return fresh_matches[-1]

    def assert_same_process_id_reused(self, processor, log_checkpoint, expected_process_id):
        log = "".join(
            self._read_lines_since(processor, processor.LOG_FILE, log_checkpoint)
        )

        reused_matches = self.reused_process_id_pattern.findall(log)

        assert expected_process_id in reused_matches, (
            "Did not see reused process id %s for %s. saw=%s"
            % (expected_process_id, processor.GROUP_INSTANCE_ID, reused_matches)
        )
        assert "Created new process id:" not in log, (
            "Unexpected random process id creation after restart for %s"
            % processor.GROUP_INSTANCE_ID
        )
        assert "No process id found on disk, got fresh process id" not in log, (
            "Unexpected fresh process id creation after restart for %s"
            % processor.GROUP_INSTANCE_ID
        )

    def assert_survivor_was_unaffected(self, processor, log_checkpoint):
        log = "".join(
            self._read_lines_since(processor, processor.LOG_FILE, log_checkpoint)
        )

        forbidden_patterns = [
            r"transitioned from STABLE to RECONCILING",
            r"Target assignment updated from",
            r"Assigned tasks with local epoch",
        ]

        for thread_instance_id in self.thread_instance_ids(processor):
            for pattern in forbidden_patterns:
                full_pattern = r"instanceId=%s.*%s" % (
                    re.escape(thread_instance_id),
                    pattern
                )
                assert not re.search(full_pattern, log), (
                    "Surviving static member %s unexpectedly logged forbidden pattern '%s' "
                    "during another member's bounce"
                    % (thread_instance_id, pattern)
                )
