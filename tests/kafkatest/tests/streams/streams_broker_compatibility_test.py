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
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsBrokerCompatibilityService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import LATEST_0_11_0, LATEST_0_10_2, LATEST_0_10_1, LATEST_0_10_0, LATEST_1_0, LATEST_1_1, \
    LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, LATEST_2_7, LATEST_2_8, \
    LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, LATEST_3_7, KafkaVersion


class StreamsBrokerCompatibility(Test):
    """
    These tests validates that
    - Streams works for older brokers 0.11 (or newer)
    - Streams w/ EOS-alpha works for older brokers 0.11 (or newer)
    - Streams w/ EOS-v2 works for older brokers 2.5 (or newer)
    - Streams fails fast for older brokers 0.10.0, 0.10.2, and 0.10.1
    - Streams w/ EOS-v2 fails fast for older brokers 2.4 or older
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
                                  },
                                  server_prop_overrides=[
                                      ["transaction.state.log.replication.factor", "1"],
                                      ["transaction.state.log.min.isr", "1"]
                                  ])
        self.consumer = VerifiableConsumer(test_context,
                                           1,
                                           self.kafka,
                                           self.output,
                                           "stream-broker-compatibility-verify-consumer")

    def setUp(self):
        self.zk.start()


    @cluster(num_nodes=4)
    @matrix(broker_version=[str(LATEST_0_11_0),str(LATEST_1_0),str(LATEST_1_1),str(LATEST_2_0),
                            str(LATEST_2_1),str(LATEST_2_2),str(LATEST_2_3),str(LATEST_2_4),
                            str(LATEST_2_5),str(LATEST_2_6),str(LATEST_2_7),str(LATEST_2_8),
                            str(LATEST_3_0),str(LATEST_3_1),str(LATEST_3_2),str(LATEST_3_3),
                            str(LATEST_3_4),str(LATEST_3_5),str(LATEST_3_6),str(LATEST_3_7)])
    def test_compatible_brokers_eos_disabled(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "at_least_once")
        processor.start()

        self.consumer.start()

        processor.wait()

        wait_until(lambda: self.consumer.total_consumed() > 0, timeout_sec=30, err_msg="Did expect to read a message but got none within 30 seconds.")

        self.consumer.stop()
        self.kafka.stop()

    @cluster(num_nodes=4)
    @matrix(broker_version=[str(LATEST_0_11_0),str(LATEST_1_0),str(LATEST_1_1),str(LATEST_2_0),
                            str(LATEST_2_1),str(LATEST_2_2),str(LATEST_2_3),str(LATEST_2_4),
                            str(LATEST_2_5),str(LATEST_2_6),str(LATEST_2_7),str(LATEST_2_8),
                            str(LATEST_3_0),str(LATEST_3_1),str(LATEST_3_2),str(LATEST_3_3),
                            str(LATEST_3_4),str(LATEST_3_5),str(LATEST_3_6),str(LATEST_3_7)])
    def test_compatible_brokers_eos_alpha_enabled(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "exactly_once")
        processor.start()

        self.consumer.start()

        processor.wait()

        wait_until(lambda: self.consumer.total_consumed() > 0, timeout_sec=30, err_msg="Did expect to read a message but got none within 30 seconds.")

        self.consumer.stop()
        self.kafka.stop()

    @cluster(num_nodes=4)
    @matrix(broker_version=[str(LATEST_2_5),str(LATEST_2_6),str(LATEST_2_7),str(LATEST_2_8),
                            str(LATEST_3_0),str(LATEST_3_1),str(LATEST_3_2),str(LATEST_3_3),
                            str(LATEST_3_4),str(LATEST_3_5),str(LATEST_3_6),str(LATEST_3_7)])
    def test_compatible_brokers_eos_v2_enabled(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "exactly_once_v2")
        processor.start()

        self.consumer.start()

        processor.wait()

        wait_until(lambda: self.consumer.total_consumed() > 0, timeout_sec=30, err_msg="Did expect to read a message but got none within 30 seconds.")

        self.consumer.stop()
        self.kafka.stop()

    @cluster(num_nodes=4)
    @parametrize(broker_version=str(LATEST_0_10_2))
    @parametrize(broker_version=str(LATEST_0_10_1))
    @parametrize(broker_version=str(LATEST_0_10_0))
    def test_fail_fast_on_incompatible_brokers(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "at_least_once")

        with processor.node.account.monitor_log(processor.STDERR_FILE) as monitor:
            processor.start()
            monitor.wait_until('FATAL: An unexpected exception org.apache.kafka.common.errors.UnsupportedVersionException',
                        timeout_sec=60,
                        err_msg="Never saw 'FATAL: An unexpected exception org.apache.kafka.common.errors.UnsupportedVersionException " + str(processor.node.account))

        self.kafka.stop()

    @cluster(num_nodes=4)
    @parametrize(broker_version=str(LATEST_2_4))
    @parametrize(broker_version=str(LATEST_2_3))
    @parametrize(broker_version=str(LATEST_2_2))
    @parametrize(broker_version=str(LATEST_2_1))
    @parametrize(broker_version=str(LATEST_2_0))
    @parametrize(broker_version=str(LATEST_1_1))
    @parametrize(broker_version=str(LATEST_1_0))
    @parametrize(broker_version=str(LATEST_0_11_0))
    def test_fail_fast_on_incompatible_brokers_if_eos_v2_enabled(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "exactly_once_v2")

        with processor.node.account.monitor_log(processor.STDERR_FILE) as monitor:
            with processor.node.account.monitor_log(processor.LOG_FILE) as log:
                processor.start()
                log.wait_until('Shutting down because the Kafka cluster seems to be on a too old version. Setting processing\.guarantee="exactly_once_v2"/"exactly_once_beta" requires broker version 2\.5 or higher\.',
                               timeout_sec=60,
                               err_msg="Never saw 'Shutting down because the Kafka cluster seems to be on a too old version. Setting `processing.guarantee=\"exactly_once_v2\"/\"exactly_once_beta\"` requires broker version 2.5 or higher.' log message " + str(processor.node.account))
                monitor.wait_until('FATAL: An unexpected exception org.apache.kafka.common.errors.UnsupportedVersionException',
                                   timeout_sec=60,
                                   err_msg="Never saw 'FATAL: An unexpected exception org.apache.kafka.common.errors.UnsupportedVersionException' error message " + str(processor.node.account))

        self.kafka.stop()
