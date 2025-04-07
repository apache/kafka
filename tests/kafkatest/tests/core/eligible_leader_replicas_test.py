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

from kafkatest.services.kafka import KafkaService, quorum, consumer_group
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int

from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

import time

class EligibleLeaderReplicasTest(Test):
    """
        Eligible leader replicas test verifies the ELR election can happen after all the replicas are offline.
        The partition will first clean shutdown 2 replicas and unclean shutdown the leader. Then the two clean shutdown
        replicas are started. One of these 2 replicas should be in ELR and become the leader.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(EligibleLeaderReplicasTest, self).__init__(test_context=test_context)

        self.topic = "input-topic"

        self.num_brokers = 6

        # Test parameters
        self.num_partitions = 1
        self.num_seed_messages = 10000

        self.progress_timeout_sec = 60
        self.consumer_group = "elr-test-consumer-group"
        self.broker_startup_timeout_sec = 120

    def seed_messages(self, topic, num_seed_messages):
        seed_timeout_sec = 10000
        seed_producer = VerifiableProducer(context=self.test_context,
                                           num_nodes=1,
                                           kafka=self.kafka,
                                           topic=topic,
                                           message_validator=is_int,
                                           max_messages=num_seed_messages,
                                           enable_idempotence=True)
        seed_producer.start()
        wait_until(lambda: seed_producer.num_acked >= num_seed_messages,
                   timeout_sec=seed_timeout_sec,
                   err_msg="Producer failed to produce messages %d in %ds." % \
                           (self.num_seed_messages, seed_timeout_sec))
        return seed_producer.acked

    def get_messages_from_topic(self, topic, num_messages, group_protocol):
        consumer = self.start_consumer(topic, group_id="verifying_consumer", group_protocol=group_protocol)
        return self.drain_consumer(consumer, num_messages)

    def stop_broker(self, node, clean_shutdown):
        if clean_shutdown:
            self.kafka.stop_node(node, clean_shutdown = True, timeout_sec = self.broker_startup_timeout_sec)
        else:
            self.kafka.stop_node(node, clean_shutdown = False)
            gracePeriodSecs = 5
            brokerSessionTimeoutSecs = 18
            wait_until(lambda: not self.kafka.pids(node),
                       timeout_sec=brokerSessionTimeoutSecs + gracePeriodSecs,
                       err_msg="Failed to see timely disappearance of process for hard-killed broker %s" % str(node.account))
            time.sleep(brokerSessionTimeoutSecs + gracePeriodSecs)

    def start_consumer(self, topic_to_read, group_id, group_protocol):
        consumer = ConsoleConsumer(context=self.test_context,
                                   num_nodes=1,
                                   kafka=self.kafka,
                                   topic=topic_to_read,
                                   group_id=group_id,
                                   message_validator=is_int,
                                   from_beginning=True,
                                   isolation_level="read_committed",
                                   consumer_properties=consumer_group.maybe_set_group_protocol(group_protocol))
        consumer.start()
        # ensure that the consumer is up.
        wait_until(lambda: (len(consumer.messages_consumed[1]) > 0) == True,
                   timeout_sec=180,
                   err_msg="Consumer failed to consume any messages for %ds" % \
                           60)
        return consumer

    def drain_consumer(self, consumer, num_messages):
        # wait until we read at least the expected number of messages.
        wait_until(lambda: len(consumer.messages_consumed[1]) >= num_messages,
                   timeout_sec=90,
                   err_msg="Consumer consumed only %d out of %d messages in %ds" % \
                           (len(consumer.messages_consumed[1]), num_messages, 90))
        consumer.stop()
        return consumer.messages_consumed[1]

    def setup_topics(self):
        self.kafka.topics = {
            self.topic: {
                "partitions": self.num_partitions,
                "replication-factor": 3,
                "configs": {
                    "min.insync.replicas": 2
                }
            }
        }

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft],
            group_protocol=consumer_group.all_group_protocols)
    def test_basic_eligible_leader_replicas(self, metadata_quorum, group_protocol=None):
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=self.num_brokers,
                                  zk=None,
                                  controller_num_nodes_override=1)
        security_protocol = 'PLAINTEXT'

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

        self.setup_topics()
        self.kafka.start(timeout_sec = self.broker_startup_timeout_sec)

        self.kafka.run_features_command("upgrade", "eligible.leader.replicas.version", 1)
        input_messages = self.seed_messages(self.topic, self.num_seed_messages)
        isr = self.kafka.isr_idx_list(self.topic, 0)

        self.stop_broker(self.kafka.nodes[isr[1] - 1], True)
        self.stop_broker(self.kafka.nodes[isr[2] - 1], True)

        wait_until(lambda: len(self.kafka.isr_idx_list(self.topic)) == 1,
                   timeout_sec=60,
                   err_msg="Timed out waiting for the partition to have only 1 ISR")

        self.stop_broker(self.kafka.nodes[isr[0] - 1], False)

        self.kafka.start_node(self.kafka.nodes[isr[1] - 1], timeout_sec = self.broker_startup_timeout_sec)
        self.kafka.start_node(self.kafka.nodes[isr[2] - 1], timeout_sec = self.broker_startup_timeout_sec)

        output_messages_set = set(self.get_messages_from_topic(self.topic, self.num_seed_messages, group_protocol))
        input_message_set = set(input_messages)

        assert input_message_set == output_messages_set, \
            "Input and concurrently consumed output message sets are not equal. Num input messages: %d. Num concurrently_consumed_messages: %d" % \
            (len(input_message_set), len(output_messages_set))