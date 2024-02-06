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

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import quorum
from kafkatest.services.streams import StreamsStandbyTaskService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest


class StreamsStandbyTask(BaseStreamsTest):
    """
    This test validates using standby tasks helps with rebalance times
    additionally verifies standby replicas continue to work in the
    face of continual changes to streams code base
    """

    streams_source_topic = "standbyTaskSource1"
    streams_sink_topic_1 = "standbyTaskSink1"
    streams_sink_topic_2 = "standbyTaskSink2"
    client_id = "stream-broker-resilience-verify-consumer"

    num_messages = 300000

    def __init__(self, test_context):
        super(StreamsStandbyTask, self).__init__(test_context,
                                                 topics={
                                                     self.streams_source_topic: {'partitions': 6,
                                                                                 'replication-factor': 1},
                                                     self.streams_sink_topic_1: {'partitions': 1,
                                                                                 'replication-factor': 1},
                                                     self.streams_sink_topic_2: {'partitions': 1,
                                                                                 'replication-factor': 1}
                                                 })

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft], use_new_coordinator=[True, False])
    def test_standby_tasks_rebalance(self, metadata_quorum, use_new_coordinator=False):
        # TODO KIP-441: consider rewriting the test for HighAvailabilityTaskAssignor
        configs = self.get_configs(
            ",sourceTopic=%s,sinkTopic1=%s,sinkTopic2=%s,internal.task.assignor.class=org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor" % (
            self.streams_source_topic,
            self.streams_sink_topic_1,
            self.streams_sink_topic_2
            )
        )

        producer = self.get_producer(self.streams_source_topic, self.num_messages, throughput=15000, repeating_keys=6)
        producer.start()

        processor_1 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)
        processor_2 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)
        processor_3 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)

        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_1.STDOUT_FILE)

        processor_2.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE)

        processor_3.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_3.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_3.STDOUT_FILE)

        processor_2.stop()

        self.wait_for_verification(processor_3, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_3.STDOUT_FILE)

        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_3.STDOUT_FILE, num_lines=2)

        processor_2.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_3.STDOUT_FILE, num_lines=2)

        processor_3.stop()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_2.STDOUT_FILE)

        processor_3.start()

        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_3.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE, num_lines=2)

        processor_1.start()
        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_2.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[1-3]", processor_3.STDOUT_FILE)

        self.assert_consume(self.client_id, "assert all messages consumed from %s" % self.streams_sink_topic_1,
                            self.streams_sink_topic_1, self.num_messages)
        self.assert_consume(self.client_id, "assert all messages consumed from %s" % self.streams_sink_topic_2,
                            self.streams_sink_topic_2, self.num_messages)

        wait_until(lambda: producer.num_acked >= self.num_messages,
                   timeout_sec=60,
                   err_msg="Failed to send all %s messages" % str(self.num_messages))

        producer.stop()

        processor_1.stop()
        processor_2.stop()
        processor_3.stop()

        # Validate the checkpoint/restore logs for monotonicity
        # This was added to ensure that standby restoration resumes from the checkpoint
        # rather than the beginning of the changelog, as part of KAFKA-9169

        # First, process the logs to look for invariant violations
        processor_1.node.account.ssh(validateMonotonicCheckpointsCmd(processor_1.LOG_FILE, processor_1.STDOUT_FILE))
        processor_2.node.account.ssh(validateMonotonicCheckpointsCmd(processor_2.LOG_FILE, processor_2.STDOUT_FILE))
        processor_3.node.account.ssh(validateMonotonicCheckpointsCmd(processor_3.LOG_FILE, processor_3.STDOUT_FILE))

        # Second, check to make sure no invariant violations were reported
        processor_1.node.account.ssh("! grep ERROR " + processor_1.STDOUT_FILE, allow_fail=False)
        processor_2.node.account.ssh("! grep ERROR " + processor_2.STDOUT_FILE, allow_fail=False)
        processor_3.node.account.ssh("! grep ERROR " + processor_3.STDOUT_FILE, allow_fail=False)


def validateMonotonicCheckpointsCmd(log_file, stdout_file):
    """
    Enforces an invariant that, if we look at the offsets written to
    checkpoint files and offsets used to resume the restore consumer,
    for a given topic/partition, we should always observe the offsets
    to be non-decreasing.
    Note that this specifically would not hold for EOS in an unclean
    shutdown, but outside of that, we should be able to rely on it.
    """
    # This script gets turned into a one-liner and executed over SSH
    #
    # The idea here is to parse the logs and enforce an invariant. This
    # should be resilient against meaningless variations in what tasks
    # exactly get assigned to which instances and other factors that could
    # make this test flaky.
    #
    # A quick overview, which should make this easier to read:
    # PK is prior key
    # PV is prior value
    #
    # 1. Extract only the relevant lines from the log (grep)
    # 2. The log lines contain a map of topic-partition -> offset,
    #    so the role of the sed expressions is to extract those map values
    #    onto one k/v pair per line.
    # 3. The sort is a _stable_ sort on the key, which puts all the
    #    events for a key together, while preserving their relative order
    # 4. Now, we use K (key), V (value), PK, and PV to make sure that
    #    the offset (V) for each topic-partition (K) is non-decreasing.
    # 5. If this invariant is violated, log an ERROR (so we can check for it later)

    return "PK=''; " \
           "PV=-999; " \
           "cat %s " \
           "| grep 'Assigning and seeking\|Writing checkpoint' " \
           "| sed -e 's/.*{\(.*\)}.*/\\1/' -e 's/, /\\n/g' -e 's/=/\\t/g' " \
           "| sort --key=1,1 --stable " \
           "| while read LINE; do" \
           "   if [[ ${LINE} ]]; then" \
           "     K=$(cut -f1 <<< ${LINE});" \
           "     V=$(cut -f2 <<< ${LINE});" \
           "     if [[ ${K} != ${PK} ]]; then" \
           "       PK=${K};" \
           "       PV=${V};" \
           "       echo \"INFO: First occurrence of ${K}; set PV=${V}.\";" \
           "     elif [[ ${V} -lt ${PV} ]]; then" \
           "       echo \"ERROR: ${K} offset regressed from ${PV} to ${V}.\"; " \
           "     else" \
           "       PV=${V};" \
           "       echo \"INFO: Updated ${K} to ${V}.\";" \
           "     fi;" \
           "   fi;" \
           "  done >> %s" % (log_file, stdout_file)
