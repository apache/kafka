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
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsUpgradeTestJobRunnerService
from kafkatest.version import LATEST_4_3


class StreamsClientTagsStatusCodeCompatibilityTest(Test):
    """
    The MISSING_CLIENT_TAGS status (code 6) was added together with version 1 of
    StreamsGroupHeartbeat. Clients released before it only support version 0 and raise on
    status codes they do not know, so the broker must withhold the status from them
    (KAFKA-20744).

    Both tests run against a broker with rack-aware assignment tags configured and a client
    that sets no client.tag.* config, which is the condition that produces the status.
    """

    RACK_AWARE_ASSIGNMENT_TAGS = "zone,cluster"

    # Printed from the processor's init(), so it only appears once a task has actually been
    # assigned - the step that breaks when a client is handed a status code it cannot parse.
    # The client discards the resulting exception without logging it, so this is the only
    # signal that distinguishes a working client from a stalled one.
    PROCESSOR_INIT_MSG = "initializing processor: topic=data"
    PROCESSED_DATA_MSG = "processed [0-9]* records from topic=data"
    CLIENT_CLOSED_MSG = "UPGRADE-TEST-CLIENT-CLOSED"
    MISSING_CLIENT_TAGS_MSG = "Missing required client tags"

    def __init__(self, test_context):
        super(StreamsClientTagsStatusCodeCompatibilityTest, self).__init__(test_context)
        self.topics = {
            'data': {'partitions': 5},
            'echo': {'partitions': 5},
        }

    def setup_kafka(self):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=1,
            zk=None,
            topics=self.topics,
            use_streams_groups=True,
            server_prop_overrides=[
                ["group.streams.min.session.timeout.ms", "10000"],
                ["group.streams.session.timeout.ms", "10000"],
                ["group.streams.rack.aware.assignment.tags", self.RACK_AWARE_ASSIGNMENT_TAGS],
            ],
        )
        self.kafka.start()
        self.kafka.run_features_command("upgrade", "streams.version", 1)

    @cluster(num_nodes=3)
    @matrix(from_version=[str(LATEST_4_3)], metadata_quorum=[quorum.combined_kraft])
    def test_version_0_client_runs_without_client_tags(self, from_version, metadata_quorum):
        """
        A client that predates the status must still be assigned tasks and process data.
        """
        self.setup_kafka()

        driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        driver.disable_auto_terminate()
        driver.start()

        processor = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        processor.set_version(from_version)
        processor.set_config("group.protocol", "streams")

        node = processor.node
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
                processor.start()
                # Guards against the version switch silently falling back to the trunk jars,
                # which would make the rest of this test vacuous.
                log_monitor.wait_until(
                    "Kafka version: " + from_version,
                    timeout_sec=60,
                    err_msg="Never saw Kafka Streams version %s on %s" % (from_version, str(node.account)))

            monitor.wait_until(
                self.PROCESSOR_INIT_MSG,
                timeout_sec=60,
                err_msg="Version %s client was never assigned a task on %s" % (from_version, str(node.account)))

            monitor.wait_until(
                self.PROCESSED_DATA_MSG,
                timeout_sec=60,
                err_msg="Version %s client never processed data on %s" % (from_version, str(node.account)))

        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.stop()
            monitor.wait_until(
                self.CLIENT_CLOSED_MSG,
                timeout_sec=60,
                err_msg="Never saw output '%s' on %s" % (self.CLIENT_CLOSED_MSG, str(node.account)))

        driver.stop()

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_latest_client_is_told_about_missing_client_tags(self, metadata_quorum):
        """
        Control case: the same broker configuration does produce the status for a client that
        negotiates version 1. Without this, a passing test above could just mean the broker
        never had the tags configured in the first place.
        """
        self.setup_kafka()

        processor = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        processor.set_version("")  # set to TRUNK
        processor.set_config("group.protocol", "streams")

        node = processor.node
        with node.account.monitor_log(processor.LOG_FILE) as log_monitor:
            processor.start()
            log_monitor.wait_until(
                self.MISSING_CLIENT_TAGS_MSG,
                timeout_sec=60,
                err_msg="Trunk client was never warned about the missing client tags on %s" % str(node.account))

        processor.stop()
