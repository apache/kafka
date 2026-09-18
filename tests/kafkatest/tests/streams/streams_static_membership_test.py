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

import json
import re
from contextlib import ExitStack

from ducktape.errors import TimeoutError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
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
    streams_application_id = "StaticMemberTestClient"

    fresh_process_id_pattern = r"No process id found on disk, got fresh process id"
    random_process_id_pattern = r"Created new process id:"

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
    @matrix(
        group_protocol=["classic", "streams"],
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_rolling_bounces_will_not_trigger_rebalance_under_static_membership(self, group_protocol, metadata_quorum):
        self.kafka.start()

        processors = self.create_processors(
            self.num_threads,
            group_protocol=group_protocol,
            persistent_process_id_store_enabled=group_protocol == self.streams_group_protocol
        )

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, self.running_message)

        self.verify_processing(processors)

        if group_protocol == self.streams_group_protocol:
            for _ in range(0, self.num_bounces):
                for bounced in processors:
                    survivors = [processor for processor in processors if processor is not bounced]

                    with ExitStack() as stack:
                        survivor_monitors = {
                            survivor: stack.enter_context(survivor.node.account.monitor_log(survivor.LOG_FILE))
                            for survivor in survivors
                        }

                        verify_stopped(bounced, self.stopped_message)
                        verify_running(bounced, self.running_message)

                        for survivor in survivors:
                            self.assert_survivor_was_unaffected(survivor, survivor_monitors[survivor])
        else:
            # do several rolling bounces
            for _ in range(0, self.num_bounces):
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

        baseline_process_ids = {}
        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, self.running_message)
            baseline_process_ids[processor] = self.read_process_id(processor)

        self.verify_processing(processors)

        for _ in range(self.num_bounces):
            for bounced in processors:
                survivors = [processor for processor in processors if processor is not bounced]

                with ExitStack() as stack:
                    reused_monitor = stack.enter_context(bounced.node.account.monitor_log(bounced.LOG_FILE))
                    bounced_forbidden_monitor = stack.enter_context(bounced.node.account.monitor_log(bounced.LOG_FILE))
                    survivor_monitors = {
                        survivor: stack.enter_context(survivor.node.account.monitor_log(survivor.LOG_FILE))
                        for survivor in survivors
                    }

                    verify_stopped(bounced, self.stopped_message)
                    verify_running(bounced, self.running_message)

                    self.assert_same_process_id_reused(
                        bounced,
                        reused_monitor,
                        bounced_forbidden_monitor,
                        baseline_process_ids[bounced]
                    )

                    for survivor in survivors:
                        self.assert_survivor_was_unaffected(survivor, survivor_monitors[survivor])

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop(timeout_sec=120)

    @cluster(num_nodes=8)
    @matrix(
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_static_member_process_id_persisted_after_rejoin(self, bounce_mode, metadata_quorum):
        self.kafka.start()

        processors = self.create_processors(
            self.num_threads,
            group_protocol=self.streams_group_protocol,
            persistent_process_id_store_enabled=True
        )

        self.producer.start()

        baseline_process_ids = {}
        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, self.running_message)
            baseline_process_ids[processor] = self.read_process_id(processor)

        self.verify_processing(processors)

        if bounce_mode == "all":
            with ExitStack() as stack:
                reused_monitors = {
                    processor: stack.enter_context(processor.node.account.monitor_log(processor.LOG_FILE))
                    for processor in processors
                }
                forbidden_monitors = {
                    processor: stack.enter_context(processor.node.account.monitor_log(processor.LOG_FILE))
                    for processor in processors
                }

                for processor in processors:
                    verify_stopped(processor, self.stopped_message)

                for processor in processors:
                    verify_running(processor, self.running_message)

                for processor in processors:
                    self.assert_same_process_id_reused(
                        processor,
                        reused_monitors[processor],
                        forbidden_monitors[processor],
                        baseline_process_ids[processor]
                    )
        else:
            for processor in processors:
                with ExitStack() as stack:
                    reused_monitor = stack.enter_context(processor.node.account.monitor_log(processor.LOG_FILE))
                    forbidden_monitor = stack.enter_context(processor.node.account.monitor_log(processor.LOG_FILE))

                    verify_stopped(processor, self.stopped_message)
                    verify_running(processor, self.running_message)

                    self.assert_same_process_id_reused(
                        processor,
                        reused_monitor,
                        forbidden_monitor,
                        baseline_process_ids[processor]
                    )

        self.verify_processing(processors)

        for node in self.kafka.nodes:
            self.kafka.restart_node(node, clean_shutdown=True, timeout_sec=120)
            assert not self.fenced_processors(processors), (
                "Static Streams member unexpectedly failed after broker rolling bounce"
            )
            self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop(timeout_sec=120)

    @cluster(num_nodes=9)
    @matrix(
        fencing_stage=["stable", "all"],
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_fencing_static_streams_member(self, fencing_stage, metadata_quorum):
        self.kafka.start()

        processors = self.create_processors(
            self.num_threads,
            group_protocol=self.streams_group_protocol
        )
        conflict_processor = StaticMemberTestService(
            self.test_context,
            self.kafka,
            processors[0].GROUP_INSTANCE_ID,
            self.num_threads,
            self.streams_group_protocol
        )

        self.producer.start()

        if fencing_stage == "stable":
            for processor in processors:
                self.set_topics(processor)
                verify_running(processor, self.running_message)

            self.verify_processing(processors)

            self.set_topics(conflict_processor)
            with ExitStack() as stack:
                survivor_monitors = {
                    processor: stack.enter_context(processor.node.account.monitor_log(processor.LOG_FILE))
                    for processor in processors
                }
                monitor = stack.enter_context(conflict_processor.node.account.monitor_log(conflict_processor.LOG_FILE))

                conflict_processor.start()
                monitor.wait_until(
                    "terminal ERROR state",
                    timeout_sec=60,
                    err_msg="Never saw the conflicting static Streams member fail"
                )

                conflict_processor.wait(timeout_sec=60)

                for processor in processors:
                    self.assert_survivor_was_unaffected(processor, survivor_monitors[processor])

            self.verify_processing(processors)

            stop_processors(processors, self.stopped_message)

            verify_running(conflict_processor, self.running_message)
            self.verify_processing([conflict_processor])
            verify_stopped(conflict_processor, self.stopped_message)
        else:
            duplicate_processors = [processors[0], conflict_processor]
            all_processors = duplicate_processors + processors[1:]

            for processor in all_processors:
                self.set_topics(processor)
                processor.start()

            wait_until(
                lambda: len(self.fenced_processors(duplicate_processors)) >= 1,
                timeout_sec=60,
                err_msg="Timed out waiting for one duplicate static Streams member to fail"
            )

            fenced_processors = self.fenced_processors(duplicate_processors)
            assert len(fenced_processors) == 1, (
                "Expected exactly one duplicate static Streams member to fail, but saw %d"
                % len(fenced_processors)
            )

            for processor in fenced_processors:
                processor.wait(timeout_sec=60)

            active_processors = [
                processor for processor in processors + [conflict_processor]
                if processor not in fenced_processors
            ]

            for processor in active_processors:
                self.wait_for_file_contains(
                    processor,
                    processor.STDOUT_FILE,
                    self.running_message,
                    "Never saw running state for active static Streams member %s"
                    % processor.GROUP_INSTANCE_ID
                )

            self.verify_processing(active_processors)

            stop_processors(active_processors, self.stopped_message)

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

    def wait_for_file_contains(self, processor, path, text, err_msg):
        wait_until(
            lambda: self.file_contains(processor, path, text),
            timeout_sec=60,
            err_msg=err_msg
        )

    def file_contains(self, processor, path, text):
        return processor.node.account.ssh(
            "grep -F --max-count 1 '%s' %s" % (text, path),
            allow_fail=True
        ) == 0

    def fenced_processors(self, processors):
        return [
            processor for processor in processors
            if self.file_contains(processor, processor.LOG_FILE, "terminal ERROR state")
        ]

    def thread_instance_ids(self, processor):
        return ["%s-%d" % (processor.GROUP_INSTANCE_ID, thread_id)
                for thread_id in range(1, self.num_threads + 1)]

    def process_id_file(self, processor):
        return "%s/%s/kafka-streams-process-metadata" % (processor.state_dir, self.streams_application_id)

    def read_process_id(self, processor):
        output = "".join(
            processor.node.account.ssh_capture(
                "cat %s" % self.process_id_file(processor),
                allow_fail=True
            )
        )
        assert output, (
            "Did not find persisted process id file for %s"
            % processor.GROUP_INSTANCE_ID
        )
        return json.loads(output)["processId"]

    def assert_not_logged(self, monitor, pattern, err_msg):
        try:
            monitor.wait_until(pattern, timeout_sec=.5, backoff_sec=.1, err_msg=err_msg)
        except TimeoutError:
            return
        raise AssertionError(err_msg)

    def assert_same_process_id_reused(self, processor, reused_monitor, forbidden_monitor, expected_process_id):
        reused_monitor.wait_until(
            r"Reading UUID from process file: %s" % re.escape(expected_process_id),
            timeout_sec=60,
            err_msg="Did not see reused process id %s for %s"
            % (expected_process_id, processor.GROUP_INSTANCE_ID)
        )

        self.assert_not_logged(
            forbidden_monitor,
            r"%s|%s" % (self.random_process_id_pattern, self.fresh_process_id_pattern),
            "Unexpected fresh/random process id creation after restart for %s"
            % processor.GROUP_INSTANCE_ID
        )

        actual_process_id = self.read_process_id(processor)
        assert actual_process_id == expected_process_id, (
            "Expected persisted process id %s for %s, but found %s"
            % (expected_process_id, processor.GROUP_INSTANCE_ID, actual_process_id)
        )

    def assert_survivor_was_unaffected(self, processor, monitor):
        forbidden_patterns = [
            r"transitioned from STABLE to RECONCILING",
            r"Target assignment updated from",
            r"Assigned tasks with local epoch",
        ]

        full_pattern = r"instanceId=(?:%s).*(?:%s)" % (
            "|".join(re.escape(thread_instance_id) for thread_instance_id in self.thread_instance_ids(processor)),
            "|".join(forbidden_patterns)
        )
        self.assert_not_logged(
            monitor,
            full_pattern,
            "Surviving static member %s unexpectedly reconciled during another member's bounce"
            % processor.GROUP_INSTANCE_ID
        )
