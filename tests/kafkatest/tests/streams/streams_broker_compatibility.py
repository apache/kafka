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

from ducktape.mark import parametrize
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsBrokerCompatibilityService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import TRUNK, LATEST_0_10_1, LATEST_0_10_0, KafkaVersion


class StreamsBrokerCompatibility(Test):
    """
    These tests validate that Streams v0.10.2+ can connect to older brokers v0.10+
    and that Streams fails fast for pre-0.10 brokers
    """

    input = "brokerCompatibilitySourceTopic"
    output = "brokerCompatibilitySinkTopic"

    def __init__(self, test_context):
        super(StreamsBrokerCompatibility, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=1,
                                  zk=self.zk,
                                  topics={
                                      self.input: {'partitions': 1, 'replication-factor': 1},
                                      self.output: {'partitions': 1, 'replication-factor': 1}
                                  })

        self.processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka)

        self.consumer = VerifiableConsumer(test_context,
                                           1,
                                           self.kafka,
                                           self.output,
                                           "stream-broker-compatibility-verify-consumer")

    def setUp(self):
        self.zk.start()

    @parametrize(broker_version=str(TRUNK))
    @parametrize(broker_version=str(LATEST_0_10_1))
    def test_compatible_brokers(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        self.processor.start()
        self.consumer.start()

        self.processor.wait()

        num_consumed_mgs = self.consumer.total_consumed()

        self.consumer.stop()
        self.kafka.stop()

        assert num_consumed_mgs == 1, \
            "Did expect to read exactly one message but got %d" % num_consumed_mgs

    @parametrize(broker_version=str(LATEST_0_10_0))
    def test_fail_fast_on_incompatible_brokers(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        self.processor.start()

        self.processor.node.account.ssh(self.processor.start_cmd(self.processor.node))
        with self.processor.node.account.monitor_log(self.processor.STDERR_FILE) as monitor:
            monitor.wait_until('Exception in thread "main" org.apache.kafka.streams.errors.StreamsException: Kafka Streams requires broker version 0.10.1.x or higher.',
                        timeout_sec=60,
                        err_msg="Never saw 'incompatible broker' error message " + str(self.processor.node.account))

        self.kafka.stop()
