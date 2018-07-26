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

from ducktape.mark import ignore
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest
from kafkatest.version import LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0,  DEV_BRANCH, KafkaVersion


class StreamsMultipleRollingUpgradeTest(BaseStreamsTest):
    """
     This test will verify a rolling upgrade of multiple streams
     applications against all versions of streams against a single
     broker version.

     As new releases come out, just update the streams_upgrade_versions array to have the latest version
     included in the list.

     A prerequisite for this test to succeed
     is the inclusion of all parametrized versions of kafka in kafka/vagrant/base.sh
     (search for get_kafka()).
     As new versions are released the kafka/tests/kafkatest/version.py file
     needs to be updated as well.

     You can find what's been uploaded to S3 with the following command

     aws s3api list-objects --bucket kafka-packages --query 'Contents[].{Key:Key}
    """
    # adding new version to this list will cover broker and streams version
    streams_upgrade_versions = [str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(DEV_BRANCH)]

    def __init__(self, test_context):
        super(StreamsMultipleRollingUpgradeTest, self).__init__(test_context,
                                                                topics={
                                                                    'echo': {'partitions': 5, 'replication-factor': 1},
                                                                    'data': {'partitions': 5, 'replication-factor': 1},
                                                                    'min': {'partitions': 5, 'replication-factor': 1},
                                                                    'max': {'partitions': 5, 'replication-factor': 1},
                                                                    'sum': {'partitions': 5, 'replication-factor': 1},
                                                                    'dif': {'partitions': 5, 'replication-factor': 1},
                                                                    'cnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'avg': {'partitions': 5, 'replication-factor': 1},
                                                                    'wcnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'tagg': {'partitions': 5, 'replication-factor': 1}
                                                                })

        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)
        self.processor_1 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_2 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_3 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)

        # already on trunk version at end of upgrades so get rid of it
        self.streams_downgrade_versions = self.streams_upgrade_versions[:-1]
        self.streams_downgrade_versions.reverse()

        self.processors = [self.processor_1, self.processor_2, self.processor_3]

        self.started = False

    def setUp(self):
        self.zk.start()

    def upgrade_and_verify_start(self, processors, to_version):
        for processor in processors:
            self.logger.info("Updating node %s to version %s" % (processor.node.account, to_version))
            node = processor.node
            if self.started:
                self.stop(processor)
            node.version = KafkaVersion(to_version)
            processor.start()
            self.wait_for_verification(processor, "initializing processor: topic", processor.STDOUT_FILE)

        self.started = True

    def stop(self, processor):
        processor.stop()
        self.wait_for_verification(processor, "SMOKE-TEST-CLIENT-CLOSED", processor.STDOUT_FILE)

    def update_processors_and_verify(self, versions):
        for version in versions:
            self.upgrade_and_verify_start(self.processors, version)
        self.run_data_and_verify()

    def run_data_and_verify(self):
        self.driver.start()
        self.wait_for_verification(self.driver, "ALL-RECORDS-DELIVERED", self.driver.STDOUT_FILE)
        self.driver.stop()

    @ignore
    @cluster(num_nodes=9)
    @matrix(broker_version=streams_upgrade_versions)
    def test_rolling_upgrade_downgrade_multiple_apps(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        # verification step run after each upgrade
        self.update_processors_and_verify(self.streams_upgrade_versions)

        # with order reversed now we test downgrading, verification run after each downgrade
        self.update_processors_and_verify(self.streams_downgrade_versions)

        for processor in self.processors:
            self.stop(processor)
