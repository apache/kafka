# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

import re

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StaticMemberPersistentProcessIdTestService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.streams.utils import verify_running, verify_stopped, stop_processors


class StreamsStaticMembershipStreamsProtocolTest(Test):
    """
    Streams group protocol specific static membership test.
    This test verifies the following behavior under the streams group protocol:

    1. The bounced static member reuses the same persisted processId.
    2. Surviving members do not reconcile because of another member's temporary bounce.
    """

    input_topic = "inputTopic"
    running_message = "REBALANCING -> RUNNING"
    stopped_message = "Static membership persistent-process-id test closed"
    processed_message = "PROCESSED"

    num_threads = 3
    num_bounces = 3
    group_protocol = "streams"

    initial_process_id_pattern = re.compile(r"No process id found on disk, got fresh process id ([0-9a-fA-F-]+)")
    random_process_id_pattern = re.compile(r"Created new process id: ([0-9a-fA-F-]+)")
    reused_process_id_pattern = re.compile(r"Reading UUID from process file: ([0-9a-fA-F-]+)")

    def __init__(self, test_context):
        super(StreamsStaticMembershipStreamsProtocolTest, self).__init__(test_context)
        self.topics = {
            self.input_topic: {"partitions": 18},
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
    def test_temporary_static_rejoin_does_not_trigger_survivor_reconciliation(self, metadata_quorum):
        self.kafka.start()

        processor1 = StaticMemberPersistentProcessIdTestService(self.test_context, self.kafka, "consumer-A", self.num_threads, self.group_protocol)
        processor2 = StaticMemberPersistentProcessIdTestService(self.test_context, self.kafka, "consumer-B", self.num_threads, self.group_protocol)
        processor3 = StaticMemberPersistentProcessIdTestService(self.test_context, self.kafka, "consumer-C", self.num_threads, self.group_protocol)

        processors = [processor1, processor2, processor3]

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            processor.INPUT_TOPIC = self.input_topic
            verify_running(processor, self.running_message)

        self.verify_processing(processors)

        baseline_process_ids = {
            processor: self.assert_initial_process_id_persisted(processor)
            for processor in processors
        }

        for _ in range(self.num_bounces):
            for bounced in processors:
                checkpoints = {
                    processor: {
                        "log": self._line_count(processor, processor.LOG_FILE),
                        "stdout": self._line_count(processor, processor.STDOUT_FILE),
                    }
                    for processor in processors
                }

                verify_stopped(bounced, self.stopped_message)
                verify_running(bounced, self.running_message)

                self.assert_same_process_id_reused(bounced, checkpoints[bounced]["log"], baseline_process_ids[bounced])

                for survivor in processors:
                    if survivor is bounced:
                        continue
                    self.assert_survivor_was_unaffected(survivor, checkpoints[survivor]["log"])

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop(timeout_sec=120)

    def verify_processing(self, processors):
        for processor in processors:
            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                monitor.wait_until(self.processed_message,
                                   timeout_sec=60,err_msg="Never saw processing of %s on %s" % (self.processed_message, str(processor.node.account))
                                   )

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

    def assert_initial_process_id_persisted(self, processor):
        log = "".join(self._read_lines_since(processor, processor.LOG_FILE, 0))

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