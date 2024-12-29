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
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StreamsBrokerCompatibilityService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.version import LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, LATEST_3_7, LATEST_3_8, LATEST_3_9, KafkaVersion


class StreamsBrokerCompatibility(Test):
    """
    These tests validates that
    - Streams w/ EOS-v2 works for older brokers 2.5 (or newer)
    """

    input = "brokerCompatibilitySourceTopic"
    output = "brokerCompatibilitySinkTopic"

    def __init__(self, test_context):
        super(StreamsBrokerCompatibility, self).__init__(test_context=test_context)
        self.kafka = KafkaService(test_context,
                                  num_nodes=1,
                                  zk=None,
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


    @cluster(num_nodes=4)
    @matrix(broker_version=[str(LATEST_3_0),str(LATEST_3_1),str(LATEST_3_2),str(LATEST_3_3),
                            str(LATEST_3_4),str(LATEST_3_5),str(LATEST_3_6),str(LATEST_3_7),
                            str(LATEST_3_8),str(LATEST_3_9)],
            metadata_quorum=[quorum.combined_kraft]
            )
    def test_compatible_brokers_eos_disabled(self, broker_version, metadata_quorum):
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
    @matrix(broker_version=[str(LATEST_3_0),str(LATEST_3_1),str(LATEST_3_2),str(LATEST_3_3),
                            str(LATEST_3_4),str(LATEST_3_5),str(LATEST_3_6),str(LATEST_3_7),
                            str(LATEST_3_8),str(LATEST_3_9)],
            metadata_quorum=[quorum.combined_kraft])
    def test_compatible_brokers_eos_v2_enabled(self, broker_version, metadata_quorum):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        processor = StreamsBrokerCompatibilityService(self.test_context, self.kafka, "exactly_once_v2")
        processor.start()

        self.consumer.start()

        processor.wait()

        wait_until(lambda: self.consumer.total_consumed() > 0, timeout_sec=30, err_msg="Did expect to read a message but got none within 30 seconds.")

        self.consumer.stop()
        self.kafka.stop()
