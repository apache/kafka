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


from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from kafkatest.services.verifiable_producer import VerifiableProducer

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.replica_verification_tool import ReplicaVerificationTool

TOPIC = "topic-replica-verification"
REPORT_INTERVAL_MS = 1000

class ReplicaVerificationToolTest(Test):
    """
    Tests ReplicaVerificationTool
    """
    def __init__(self, test_context):
        super(ReplicaVerificationToolTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 2
        self.messages_received_count = 0
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 2}
        }

        self.zk = ZookeeperService(test_context, self.num_zk)
        self.kafka = None
        self.producer = None
        self.replica_verifier = None

    def setUp(self):
        self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics)
        self.kafka.start()

    def start_replica_verification_tool(self, security_protocol):
        self.replica_verifier = ReplicaVerificationTool(self.test_context, 1, self.kafka, TOPIC, report_interval_ms=REPORT_INTERVAL_MS, security_protocol=security_protocol)
        self.replica_verifier.start()

    def start_producer(self, max_messages, acks, timeout):
        # This will produce to kafka cluster
        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=TOPIC, throughput=1000, acks=acks, max_messages=max_messages)
        current_acked = self.producer.num_acked
        self.logger.info("current_acked = %s" % current_acked)
        self.producer.start()
        wait_until(lambda: acks == 0 or self.producer.num_acked >= current_acked + max_messages, timeout_sec=timeout,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def stop_producer(self):
        self.producer.stop()

    def test_replica_lags(self, security_protocol='PLAINTEXT'):
        """
        Tests ReplicaVerificationTool
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_replica_verification_tool(security_protocol)
        self.start_producer(max_messages=10, acks=-1, timeout=15)
        # Verify that there is no lag in replicas and is correctly reported by ReplicaVerificationTool
        wait_until(lambda: self.replica_verifier.get_lag_for_partition(TOPIC, 0) == 0, timeout_sec=10,
                   err_msg="Timed out waiting to reach zero replica lags.")
        self.stop_producer()

        self.start_producer(max_messages=1000, acks=0, timeout=5)
        # Verify that there is lag in replicas and is correctly reported by ReplicaVerificationTool
        wait_until(lambda: self.replica_verifier.get_lag_for_partition(TOPIC, 0) > 0, timeout_sec=10,
                   err_msg="Timed out waiting to reach non-zero number of replica lags.")